"""集中管理環境變數設定(非機密的預設值)。
必填機密(DEEPSEEK_API_KEY / NVIDIA_API_KEY)刻意仍由各使用處以 os.environ[...] 讀取,
保留「缺少即 fail-fast」語意,不在此給預設空值。"""
import os
from pathlib import Path

FALKOR_HOST = os.getenv("FALKOR_HOST", "127.0.0.1")
FALKOR_PORT = int(os.getenv("FALKOR_PORT", "6379"))
FALKOR_DB = os.getenv("FALKOR_DB", "news")
GROUP_ID = os.getenv("NEWS_GROUP_ID", "news")
WEB_HOST = os.getenv("NEWSGRAPH_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("NEWSGRAPH_PORT", "8080"))
API_KEY = os.getenv("NEWSGRAPH_API_KEY", "")
QUEUE_FILE = Path(os.getenv("NEWSGRAPH_QUEUE", "/app/state/queue.json"))
RECENT_DAYS = float(os.getenv("RECENT_DAYS", "1"))
