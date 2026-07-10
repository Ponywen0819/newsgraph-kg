FROM docker.io/library/python:3.12-slim
ENV PIP_NO_CACHE_DIR=1 PYTHONUNBUFFERED=1
# 統一映像(取代舊的 newsgraph + newsgraph-viz 兩個映像):
#   graphiti-core: 知識圖譜引擎([falkordb]圖驅動、[google-genai] 選用 embedder/reranker)
#   opencc-python-reimplemented: 純 Python 繁簡轉換(無 C 依賴、ARM 友善)
#   fastapi[standard]+uvicorn: 統一 Web 服務(寫入 API + 唯讀視覺化)
#   falkordb: 唯讀讀取層 graphrepo 直查圖(不透過 Graphiti)
#   feedparser: 保留給測試用的 RSS collector
RUN pip install "graphiti-core[falkordb,google-genai]" opencc-python-reimplemented \
    "fastapi[standard]" uvicorn falkordb feedparser
WORKDIR /app
# 程式碼 COPY 進映像(可重現部署)。CLI 開發時 run.sh 仍可 mount $PWD 覆蓋成即時程式碼。
COPY newsgraph ./newsgraph
COPY sample_news.json ./
EXPOSE 8080
# 預設進入點 python:
#   Web 服務 → Quadlet Exec: -m uvicorn newsgraph.web.app:app --host 0.0.0.0 --port 8080
#   CLI     → run.sh:        -m newsgraph.ingest / .reporter / .query …
ENTRYPOINT ["python"]
