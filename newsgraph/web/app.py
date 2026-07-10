"""統一 FastAPI app(單一埠 8080)。

- 寫入端點需 X-API-Key(routes_ingest);讀取/視覺化公開(routes_view)。
- lifespan:startup 啟動常駐 async worker(queue.supervisor);shutdown 取消並乾淨關閉 Graphiti。
- GET / 回 static/index.html;/static 掛靜態檔。

正式部署由 Quadlet 指定 host/port(P3);本檔 __main__ 僅供本機測試。
"""
import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .. import config
from .. import queue
from .routes_ingest import router as ingest_router
from .routes_view import router as view_router

HERE = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(HERE, "static")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup:啟動常駐 supervisor(內部建 Graphiti → 跑 worker,失敗節流重建)。
    task = asyncio.create_task(queue.supervisor(), name="newsgraph-worker")
    app.state.worker_task = task
    try:
        yield
    finally:
        # shutdown:取消常駐 task 並等它收回(supervisor 收到 CancelledError 會乾淨往外傳),
        # 再由此關閉 supervisor 當下持有的 Graphiti(driver + async client)。
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except BaseException:
            pass
        g = queue.current_graphiti()
        if g is not None:
            await queue._aclose_graphiti(g)


app = FastAPI(title="NewsGraph", lifespan=lifespan)

app.include_router(ingest_router)
app.include_router(view_router)


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.WEB_HOST, port=config.WEB_PORT)
