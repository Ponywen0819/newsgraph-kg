"""NewsGraph 薄 API：給 n8n 呼叫。生產者/消費者佇列版（記憶體友善、序列灌圖、不漏資料）。

  POST /queue/push   body={items:[...], done_webhook:"url"}
       → 把文章 append 進 host 佇列檔，啟動背景 drainer；立刻回。
  背景 drainer：佇列非空就一篇一篇 pop→灌圖（序列，~60s/篇）→排空後打 done_webhook 通知（→ n8n 簡報 workflow）。
  GET  /delta?hours=N   → 新增/被推翻的事實（給 n8n 的 LLM 寫簡報）
  GET  /queue           → {pending, draining}
  POST /ingest          → 直接同步灌一批（測試用）
  GET  /health

驗證：/queue/* /ingest /delta 需 X-API-Key: <NEWSGRAPH_API_KEY>（綁 0.0.0.0 讓 n8n 連得到，token 擋 LAN）。
Network=host 直連 FalkorDB 127.0.0.1:6379；序列灌圖靠 MAX_COROUTINES=1 + 逐篇容錯（見 ingest.py）。
"""
import asyncio
import json
import os
import threading
import traceback
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from .ingest import run as ingest_run
from .graph import build_graphiti
from .delta import compute_delta

API_KEY = os.environ.get("NEWSGRAPH_API_KEY", "")
PORT = int(os.getenv("NEWSGRAPH_API_PORT", "8090"))
QUEUE_FILE = Path(os.getenv("NEWSGRAPH_QUEUE", "/app/state/queue.json"))

_lock = threading.Lock()   # 保護佇列檔的 read-modify-write 與 _draining 旗標
_draining = False


def _read_queue():
    if QUEUE_FILE.exists():
        try:
            return json.loads(QUEUE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"items": [], "done_webhook": None}


def _write_queue(q):
    QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = QUEUE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(q, ensure_ascii=False), encoding="utf-8")
    tmp.replace(QUEUE_FILE)   # 原子替換，避免半寫壞檔


def _post_webhook(url, payload):
    try:
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                     method="POST", headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=30).read()
        print(f"[drain] 已通知 done_webhook：{url}", flush=True)
    except Exception as e:
        print(f"[drain] done_webhook 失敗：{e}", flush=True)


async def _aclose_graphiti(g):
    """乾淨關閉：圖 driver + 底層 openai/httpx async client。
    只關 driver 不夠——llm_client / embedder / cross_encoder 各自持有一個 AsyncOpenAI
    連線池，若不在事件圈內主動關閉，事件圈一收掉，其 finalizer 會在已關閉的 loop 上
    排 aclose()，噴一堆 'Event loop is closed' traceback（無害但很吵）。
    另外 FalkorDriver 會「自動背景建索引」——那個 fire-and-forget task 若在關連線後才
    收尾，會噴 'Task exception was never retrieved'（Connection closed by server）。
    因此關 driver 前先把還在跑的背景 task 等完並取回其例外。"""
    try:
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if pending:
            await asyncio.wait(pending, timeout=30)
            for t in pending:              # 取回例外 → 消除 "never retrieved" 警告
                if t.done() and not t.cancelled():
                    t.exception()
    except Exception:
        pass
    try:
        await g.close()   # 關圖 driver
    except Exception:
        pass
    for comp in (getattr(g, "llm_client", None),
                 getattr(g, "embedder", None),
                 getattr(g, "cross_encoder", None)):
        closer = getattr(getattr(comp, "client", None), "close", None)
        if closer:
            try:
                await closer()
            except Exception:
                pass


async def _adrain(graphiti):
    """在單一事件圈內序列排空佇列（一篇一篇灌，共用同一個 Graphiti 實例）。
    回傳 (processed, failed, attempted, webhook)。

    _draining=False 必須在「觀察到佇列空」的同一次持鎖中設定並跳出，否則會與 _enqueue
    的『是否要另起 drainer』判斷產生漏喚醒——這是正常路徑的交接點，勿刪。

    不漏資料（Bug 2）：持鎖時只 peek 佇列前端（不移除、不寫檔）；等 ingest 回來之後才
    再持鎖 pop(0) 移除。若容器在 ingest 途中被殺，該篇仍留在 queue.json，下次重跑靠
    ingest.py 既有的 seen.txt 去重達成 at-least-once（重跑已 seen 的會被跳過）。
    正確性：全程只有這唯一一個 drainer 會從佇列「前端」移除，而 _enqueue 只會 append 到
    尾端，所以 ingest 後重讀佇列、pop(0) 移除的必然仍是剛處理的那一篇。
    毒丸處理：ingest_run 明確拋 Exception（單篇 DeepSeek 出包等）視為失敗跳過，仍要把該篇
    移除（否則壞掉那篇會永遠卡在前端擋住後面全部）；只有崩潰（BaseException／程序被殺）
    才會跳過移除、把該篇留著重試。"""
    global _draining
    processed, failed, attempted, webhook = 0, 0, 0, None
    while True:
        with _lock:                       # 區塊內無 await → 對事件圈與其他執行緒都是原子的
            q = _read_queue()
            webhook = q.get("done_webhook")
            if not q["items"]:
                _draining = False         # 交接點：觀察到空的當下就交回旗標，防 lost-wakeup
                break
            article = q["items"][0]        # 只 peek，不移除也不寫檔（崩潰前不丟資料）
        attempted += 1
        title = (article.get("title") or "")[:50]
        try:
            await ingest_run(graphiti, [article], close=False)   # 一次一篇，不關實例
            processed += 1
            print(f"[drain] 灌完第 {processed} 篇：{title}", flush=True)
        except Exception as e:            # 只吞 Exception：明確失敗＝毒丸→仍移除；BaseException（崩潰）會往外拋、跳過下面移除
            failed += 1
            print(f"[drain] 失敗跳過：{title}：{str(e)[:160]}", flush=True)
        with _lock:                       # ingest 回來後（成功 or 明確失敗）才把該篇移除
            q = _read_queue()
            if q["items"]:
                q["items"].pop(0)          # 前端唯一移除者是本 drainer，故仍是剛處理那篇
                _write_queue(q)
    return processed, failed, attempted, webhook


def _drain_loop():
    """背景執行緒：整個排空過程共用一個事件圈與一個 Graphiti 實例，
    排空後（只要這一輪有嘗試過任何篇）打 done_webhook。

    健壯性（Bug 1）：整段包在 try/except/finally 裡。若 drainer 中途死亡（例如 push 到來時
    FalkorDB 剛好連不上 → build_graphiti() 拋例外 → asyncio.run 拋出），不可讓執行緒靜默
    崩潰，也不可讓 _draining 永遠卡在 True——否則之後每次 push 都判定『已有 drainer』而不再
    啟動 → queue.json 無限累積、每日管線無聲死亡。finally 兜底把 _draining 設回 False，保證
    未來的 push 能重啟 drainer；殘留的佇列項目留在 queue.json，下次 push 進來時自然被新
    drainer 接手（不在此處緊迴圈重試，以免 build_graphiti 永遠失敗時變成瘋狂 crash-loop）。"""
    global _draining
    result = None
    handed_off = False
    try:
        async def _main():
            nonlocal handed_off
            graphiti = build_graphiti()
            try:
                r = await _adrain(graphiti)
                handed_off = True   # _adrain 正常返回 → 已在持鎖觀察空佇列時把 _draining 交接掉
                return r
            finally:
                await _aclose_graphiti(graphiti)
        result = asyncio.run(_main())
    except BaseException as e:       # 大聲記錄異常結束，不要讓執行緒靜默崩潰
        print(f"[drain] drainer 異常結束（將由下次 push 重啟）：{e}", flush=True)
        traceback.print_exc()
    finally:
        # 兜底安全網：只在「未正常交接」（即崩潰）時重置，避免與已接手的後繼 drainer 搶旗標。
        # 正常路徑下 _adrain 已在交接點把 _draining 設為 False，handed_off=True，此處為 no-op。
        if not handed_off:
            with _lock:
                _draining = False

    if result:
        processed, failed, attempted, webhook = result
        print(f"[drain] 佇列排空，共嘗試 {attempted} 篇（成功 {processed}、失敗 {failed}）", flush=True)
        # Bug 3：只要有取出嘗試過（attempted>0）就通知，即使整批全部灌圖失敗（processed==0）；
        # 否則下游 News Briefing 會被靜默跳過，即使時間窗內其實還有可報內容。佇列本來就空、
        # 沒取出任何東西（attempted==0）時才不打。
        if attempted and webhook:
            _post_webhook(webhook, {"processed": processed, "failed": failed, "attempted": attempted})


def _enqueue(items, done_webhook):
    global _draining
    with _lock:
        q = _read_queue()
        q["items"].extend(items)
        if done_webhook:
            q["done_webhook"] = done_webhook
        total = len(q["items"])
        _write_queue(q)
        start = not _draining
        if start:
            _draining = True
    if start:
        threading.Thread(target=_drain_loop, daemon=True).start()
    return total


def _articles(data):
    arts = data.get("items", data) if isinstance(data, dict) else data
    return arts if isinstance(arts, list) else []


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authed(self):
        if not API_KEY or self.headers.get("X-API-Key") == API_KEY:
            return True
        self._send(401, {"error": "unauthorized"})
        return False

    def _read_body(self):
        n = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(n).decode("utf-8")) if n else None

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/health":
            return self._send(200, {"ok": True})
        if not self._authed():
            return
        if u.path == "/delta":
            hours = int(parse_qs(u.query).get("hours", ["12"])[0])
            try:
                return self._send(200, compute_delta(hours))
            except Exception as e:
                return self._send(500, {"error": f"delta failed: {e}"})
        if u.path == "/queue":
            with _lock:
                pending = len(_read_queue()["items"])
            return self._send(200, {"pending": pending, "draining": _draining})
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        u = urlparse(self.path)
        if not self._authed():
            return
        try:
            data = self._read_body()
        except Exception as e:
            return self._send(400, {"error": f"invalid json: {e}"})

        if u.path == "/queue/push":
            items = _articles(data)
            if not items:
                return self._send(400, {"error": "no articles"})
            webhook = data.get("done_webhook") if isinstance(data, dict) else None
            total = _enqueue(items, webhook)
            return self._send(200, {"ok": True, "accepted": len(items), "queued": total})

        if u.path == "/ingest":   # 同步灌一批（測試/手動用）
            arts = _articles(data)
            if not arts:
                return self._send(400, {"error": "no articles"})
            async def _one_shot():
                g = build_graphiti()
                try:
                    await ingest_run(g, arts, close=False)
                finally:
                    await _aclose_graphiti(g)
            try:
                asyncio.run(_one_shot())
                return self._send(200, {"ok": True, "submitted": len(arts)})
            except Exception as e:
                return self._send(500, {"ok": False, "error": str(e)[:400]})

        return self._send(404, {"error": "not found"})

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    print(f"[newsgraph-api] listening on 0.0.0.0:{PORT} (auth={'on' if API_KEY else 'OFF'}) queue={QUEUE_FILE}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
