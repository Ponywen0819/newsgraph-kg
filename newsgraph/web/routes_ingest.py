"""寫入 / 佇列 / delta API(APIRouter)。除 /health 外皆需 X-API-Key。

自舊 api.py 的 do_GET/do_POST 移植而來,語意對齊:
  - 驗證:X-API-Key == config.API_KEY;config.API_KEY 為空字串則放行(維持現行語意)。
  - POST /queue/push  (auth):入列給 lifespan async worker 排空。
  - POST /ingest      (auth,測試用):【語意變更】改為「入列」而非同步灌,避免另開一個
                       Graphiti 實例與 worker 併發寫同一張圖。
  - GET  /delta       (auth):同步 def → FastAPI 丟 threadpool,不阻塞事件圈。
  - GET  /queue       (auth):佇列狀態。
  - GET  /health      (公開)。
"""
from fastapi import APIRouter, Depends, Header, HTTPException, Body

from .. import config
from .. import queue
from ..delta import compute_delta

router = APIRouter()


def require_api_key(x_api_key: str | None = Header(default=None)):
    """auth 依賴:config.API_KEY 為空則放行;否則需 header X-API-Key 完全相符。"""
    if not config.API_KEY or x_api_key == config.API_KEY:
        return True
    raise HTTPException(status_code=401, detail="unauthorized")


def _articles(data):
    """容錯:dict 有 items 取 items,否則 data 本身若是 list 就用它。"""
    arts = data.get("items", data) if isinstance(data, dict) else data
    return arts if isinstance(arts, list) else []


@router.get("/health")
def health():
    """公開健康檢查(不套 auth)。"""
    return {"ok": True}


@router.post("/queue/push", dependencies=[Depends(require_api_key)])
async def queue_push(data=Body(default=None)):
    """body {items:[...], done_webhook} → 入列 → {ok, accepted, queued}。"""
    items = _articles(data)
    if not items:
        raise HTTPException(status_code=400, detail="no articles")
    webhook = data.get("done_webhook") if isinstance(data, dict) else None
    total = await queue.push(items, webhook)
    return {"ok": True, "accepted": len(items), "queued": total}


@router.post("/ingest", dependencies=[Depends(require_api_key)])
async def ingest(data=Body(default=None)):
    """測試/手動用。【語意變更】原本是同步灌一批;現改為「入列」,交由同一個 lifespan
    worker(單一 Graphiti 實例、序列)處理,避免另開 Graphiti 與 worker 併發寫同一張圖。"""
    items = _articles(data)
    if not items:
        raise HTTPException(status_code=400, detail="no articles")
    total = await queue.push(items, None)
    return {"ok": True, "queued": total}


@router.get("/delta", dependencies=[Depends(require_api_key)])
def delta(hours: int = 12):
    """新增/被推翻的事實(給 n8n 的 LLM 寫簡報)。同步 def → threadpool。"""
    try:
        return compute_delta(hours)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"delta failed: {e}")


@router.get("/queue", dependencies=[Depends(require_api_key)])
def queue_status():
    """佇列狀態:pending=佇列長度,working=worker 是否在忙。"""
    return {"pending": queue.pending_count(), "working": queue.is_working()}
