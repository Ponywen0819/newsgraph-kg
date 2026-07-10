"""持久佇列 + FastAPI lifespan 內的長駐 async worker（P2）。

取代舊 api.py 的「執行緒 drainer」：改為 uvicorn 事件圈內的常駐 asyncio task,
靠 asyncio.Event 被 /queue/push 喚醒,持有單一 Graphiti 實例序列灌圖。

必須保住 PR#4 三保證(見 docs/refactor-unify-service.md 第 7 節):
  - Bug2 崩潰不遺失:持 lock 只 peek 佇列首篇(不移除)→ 放 lock → ingest →
    ingest 回來後才持 lock pop(0)+write。程序被殺(BaseException/不經 except)時該篇
    仍留在 queue.json,下次靠 ingest 的 seen.txt 去重(at-least-once)。
  - Bug2 毒丸:ingest_run 明確拋 Exception(單篇 DeepSeek 出包等)→ 記 failed 但「仍
    pop 移除」,否則壞掉那篇永遠卡在前端擋住整個佇列;只有 BaseException 才往外傳、不移除。
  - Bug1 不永久卡死:worker 是常駐 task、不使用 _draining 旗標;task 異常結束由
    supervisor 節流重建(build_graphiti/FalkorDB 暫時失敗不會讓灌圖永久停擺、也不緊迴圈)。
  - Bug3 全失敗仍觸發 webhook:以 attempted>0 判斷,payload {processed, failed, attempted}。
"""
import asyncio
import json
import os
import traceback
import urllib.request

from . import config
from .ingest import run as ingest_run
from .graph import build_graphiti

# 單一事件圈內的協調原語(取代原 threading.Lock)。
# 在 Python 3.10+ 這些物件建立時不綁定特定 loop,可安全於模組載入時建立。
_lock = asyncio.Lock()            # 保護佇列檔的 read-modify-write
_wake = asyncio.Event()           # /queue/push → wake.set();worker await wake.wait()

_working = False                  # worker 是否正在灌某一篇(供 GET /queue 顯示)
_current_graphiti = None          # supervisor 目前持有的 Graphiti 實例(供 lifespan shutdown 關閉)

# build_graphiti / FalkorDB 暫時失敗時的節流重試間隔(秒),避免瘋狂 crash-loop。
RETRY_DELAY = float(os.getenv("NEWSGRAPH_WORKER_RETRY_SEC", "5"))


# ---------------------------------------------------------------------------
# 佇列檔存取(原子寫)
# ---------------------------------------------------------------------------
def read_queue():
    qf = config.QUEUE_FILE
    if qf.exists():
        try:
            return json.loads(qf.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"items": [], "done_webhook": None}


def write_queue(q):
    qf = config.QUEUE_FILE
    qf.parent.mkdir(parents=True, exist_ok=True)
    tmp = qf.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(q, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, qf)   # 原子替換,避免半寫壞檔


def pending_count() -> int:
    return len(read_queue().get("items", []))


def is_working() -> bool:
    return _working


def current_graphiti():
    return _current_graphiti


# ---------------------------------------------------------------------------
# webhook(避免阻塞事件圈 → 丟 executor)
# ---------------------------------------------------------------------------
def _post_sync(url, payload):
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode("utf-8"),
            method="POST", headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=30).read()
        print(f"[worker] 已通知 done_webhook:{url}", flush=True)
    except Exception as e:
        print(f"[worker] done_webhook 失敗:{e}", flush=True)


async def post_webhook(url, payload):
    """同步的 urllib POST 丟到 threadpool,不阻塞事件圈。"""
    await asyncio.to_thread(_post_sync, url, payload)


# ---------------------------------------------------------------------------
# 乾淨關閉 Graphiti(整段自 api.py 移植,供 lifespan shutdown / supervisor 重建用)
# ---------------------------------------------------------------------------
async def _aclose_graphiti(g):
    """乾淨關閉:圖 driver + 底層 openai/httpx async client。
    只關 driver 不夠——llm_client / embedder / cross_encoder 各自持有一個 AsyncOpenAI
    連線池,若不在事件圈內主動關閉,事件圈一收掉,其 finalizer 會在已關閉的 loop 上
    排 aclose(),噴一堆 'Event loop is closed' traceback(無害但很吵)。
    另外 FalkorDriver 會「自動背景建索引」——那個 fire-and-forget task 若在關連線後才
    收尾,會噴 'Task exception was never retrieved'(Connection closed by server)。
    (在共享的 uvicorn loop 下不再掃 asyncio.all_tasks()——見下方說明。)

    註:api.py 舊版跑在 asyncio.run() 專屬 loop,all_tasks() 只含 graphiti 的背景 task,
    故可安全等它收尾。移到統一 app 後改由 lifespan/supervisor 於 uvicorn 共享 loop 呼叫,
    all_tasks() 會混入 uvicorn 自身的長駐 task,asyncio.wait(timeout=30) 會空等到逾時、
    拖慢每次 shutdown/重建。而 FalkorDriver 的自動建索引 task 在長駐服務啟動時早已完成,
    此處不需再等,故直接關閉 driver 與 client。"""
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


# ---------------------------------------------------------------------------
# 佇列寫入
# ---------------------------------------------------------------------------
async def push(items, done_webhook=None) -> int:
    """append 進佇列、設 webhook、write,然後喚醒 worker。回傳佇列總長度。"""
    async with _lock:                     # 區塊內無 await 觸及佇列檔 RMW → 單一 loop 內原子
        q = read_queue()
        q["items"].extend(items)
        if done_webhook:
            q["done_webhook"] = done_webhook
        total = len(q["items"])
        write_queue(q)
    _wake.set()
    return total


# ---------------------------------------------------------------------------
# 長駐 worker + supervisor
# ---------------------------------------------------------------------------
async def worker(graphiti):
    """常駐迴圈:被喚醒後排空佇列,排空(這一輪有嘗試過)才打 done_webhook。
    永不正常返回;只在 ingest 拋 BaseException(崩潰)時把例外往外傳給 supervisor。"""
    global _working
    while True:
        await _wake.wait()
        _wake.clear()                     # 先清旗標再排空;內層每次重讀佇列檔,故排空期間新
                                          # push 的項目(append 到尾端 + 再 set)都會被接手,不漏喚醒。
        processed = failed = attempted = 0
        webhook = None
        while True:
            async with _lock:             # 只 peek 佇列首篇,不移除也不寫檔(崩潰前不丟資料)
                q = read_queue()
                webhook = q.get("done_webhook")
                if not q["items"]:
                    break
                article = q["items"][0]
            attempted += 1
            _working = True
            title = (article.get("title") or "")[:50]
            try:
                await ingest_run(graphiti, [article], close=False)   # 一次一篇,不關實例
                processed += 1
                print(f"[worker] 灌完第 {processed} 篇:{title}", flush=True)
            except Exception as e:        # 只吞 Exception:明確失敗＝毒丸 → 仍移除(見下);
                failed += 1               # BaseException(程序被殺/取消)會略過下面 pop、往外傳
                print(f"[worker] 失敗跳過:{title}:{str(e)[:160]}", flush=True)
            async with _lock:             # ingest 回來後(成功 or 明確失敗)才把該篇移除
                q = read_queue()
                if q["items"]:
                    q["items"].pop(0)     # 前端唯一移除者是本 worker,push 只 append 尾端,
                    write_queue(q)        # 故重讀後 pop(0) 移除的必然仍是剛處理的那一篇
            _working = False
        _working = False
        # Bug3:只要取出嘗試過(attempted>0)就通知,即使整批全部灌圖失敗(processed==0)。
        if attempted and webhook:
            print(f"[worker] 佇列排空,共嘗試 {attempted} 篇(成功 {processed}、失敗 {failed})", flush=True)
            await post_webhook(webhook, {"processed": processed, "failed": failed, "attempted": attempted})


async def supervisor():
    """常駐監督者:建 Graphiti → 跑 worker;worker 因例外結束(如 FalkorDB 連不上、
    build_graphiti 失敗)時節流 sleep 後重建,使暫時性失敗不會讓灌圖永久停擺、也不緊迴圈。
    收到 CancelledError(app shutdown)則乾淨往外傳,由 lifespan 負責關閉 Graphiti。"""
    global _current_graphiti, _working
    while True:
        # 每輪起手先確保喚醒旗標為 set:worker 因例外中斷、佇列仍有殘留項目時,重建後才
        # 會立刻把殘留排空,而不必枯等下一次 push(空佇列時 worker 只會空轉一圈,無害)。
        _wake.set()
        try:
            g = build_graphiti()
            _current_graphiti = g
            await worker(g)               # 正常不返回;返回或拋例外都進 except 收尾
        except asyncio.CancelledError:
            raise                         # shutdown:交回 lifespan,由它 aclose _current_graphiti
        except BaseException as e:
            _working = False
            print(f"[worker] supervisor 捕獲例外,{RETRY_DELAY}s 後重建:{e}", flush=True)
            traceback.print_exc()
            g, _current_graphiti = _current_graphiti, None
            if g is not None:
                try:
                    await _aclose_graphiti(g)
                except BaseException:
                    pass
            await asyncio.sleep(RETRY_DELAY)   # 節流,避免 build_graphiti 一直失敗時瘋狂重試
