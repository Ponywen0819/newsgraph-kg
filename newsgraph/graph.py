"""建立設定好的 Graphiti 實例：
   - 抽取 LLM：DeepSeek（OpenAI 相容）
   - Embedding：NVIDIA NIM nv-embed-v1（自訂 embedder）
   - Reranker：DeepSeek（基本 RRF 搜尋不會真的用到它，僅為滿足建構子）
   - 存儲：FalkorDB
"""
import os
from graphiti_core import Graphiti
from graphiti_core.driver.falkordb_driver import FalkorDriver
from graphiti_core.llm_client.config import LLMConfig
from graphiti_core.cross_encoder.openai_reranker_client import OpenAIRerankerClient

from .clients.nvidia_embedder import NvidiaEmbedder
from .clients.deepseek import DeepSeekClient

# 整個圖的命名空間；之後要分多個圖（如不同主題）可改這裡
GROUP_ID = os.getenv("NEWS_GROUP_ID", "news")


def build_graphiti() -> Graphiti:
    nvidia_key = os.environ["NVIDIA_API_KEY"]
    nvidia_base = os.getenv("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1")
    provider = os.getenv("LLM_PROVIDER", "deepseek").lower()

    driver = FalkorDriver(
        host=os.getenv("FALKOR_HOST", "127.0.0.1"),
        port=int(os.getenv("FALKOR_PORT", "6379")),
        database=os.getenv("FALKOR_DB", "news"),
    )

    # 抽取 LLM：可用 LLM_PROVIDER 切換
    if provider == "nvidia":
        # NVIDIA 託管模型（如 minimax-m3）原生支援 json_schema，不需攤平 hack
        llm_config = LLMConfig(
            api_key=nvidia_key,
            model=os.getenv("NVIDIA_LLM_MODEL", "minimaxai/minimax-m3"),
            base_url=nvidia_base,
        )
        from graphiti_core.llm_client.openai_generic_client import OpenAIGenericClient
        llm = OpenAIGenericClient(config=llm_config, structured_output_mode="json_schema")
    else:
        # DeepSeek：只支援 json_object，並用子類攤平多包的 properties
        llm_config = LLMConfig(
            api_key=os.environ["DEEPSEEK_API_KEY"],
            model=os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
            base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
        )
        llm = DeepSeekClient(config=llm_config, structured_output_mode="json_object")

    embedder = NvidiaEmbedder(
        api_key=nvidia_key,
        model=os.getenv("NVIDIA_EMBED_MODEL", "nvidia/nv-embed-v1"),
        base_url=nvidia_base,
        input_type=os.getenv("NVIDIA_INPUT_TYPE", "passage"),
        truncate=os.getenv("NVIDIA_TRUNCATE", "END"),
    )

    # reranker 用同一個 provider 的 config（基本搜尋走 RRF 不會真的呼叫它）
    reranker = OpenAIRerankerClient(config=llm_config)

    return Graphiti(
        graph_driver=driver,
        llm_client=llm,
        embedder=embedder,
        cross_encoder=reranker,
        max_coroutines=int(os.getenv("MAX_COROUTINES", "3")),
    )
