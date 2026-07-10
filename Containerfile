FROM docker.io/library/python:3.12-slim
ENV PIP_NO_CACHE_DIR=1 PYTHONUNBUFFERED=1
# graphiti-core: 知識圖譜引擎；[falkordb]=圖驅動, [google-genai]=Gemini embedder/reranker
# feedparser: 測試用的 RSS collector（之後可換成你的黑盒 collector）
# opencc-python-reimplemented: 純 Python 繁簡轉換（無 C 依賴、ARM 友善），統一成台灣繁體以改善去重
RUN pip install "graphiti-core[falkordb,google-genai]" feedparser opencc-python-reimplemented
WORKDIR /app
ENTRYPOINT ["python"]
