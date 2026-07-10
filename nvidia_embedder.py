"""NVIDIA NIM embedding（nvidia/nv-embed-v1）的 Graphiti embedder。

為什麼要自訂：NVIDIA 的 /v1/embeddings 需要 OpenAI 規格外的欄位 input_type
（query/passage）與 truncate，Graphiti 內建的 OpenAIEmbedder 不會送這些。

注意（已知限制）：Graphiti 對 query 與 document 是「對稱呼叫」同一個 embedder，
無法乾淨地區分，因此這裡對所有文字統一用同一個 input_type（預設 passage），
以保證整個向量空間一致。nv-embed 的非對稱檢索優勢在這套架構下用不到。
"""
import os
from openai import AsyncOpenAI
from graphiti_core.embedder.client import EmbedderClient


class NvidiaEmbedder(EmbedderClient):
    def __init__(
        self,
        api_key: str,
        model: str = "nvidia/nv-embed-v1",
        base_url: str = "https://integrate.api.nvidia.com/v1",
        input_type: str = "passage",
        truncate: str = "END",
        batch_size: int = 50,
    ):
        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self.model = model
        self.input_type = input_type
        self.truncate = truncate
        self.batch_size = batch_size

    async def _embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for i in range(0, len(texts), self.batch_size):
            chunk = [t if isinstance(t, str) else str(t) for t in texts[i : i + self.batch_size]]
            resp = await self.client.embeddings.create(
                model=self.model,
                input=chunk,
                encoding_format="float",
                extra_body={"input_type": self.input_type, "truncate": self.truncate},
            )
            # 保險：依 index 排序，避免回傳順序不一致
            data = sorted(resp.data, key=lambda d: d.index)
            out.extend([d.embedding for d in data])
        return out

    async def create(self, input_data) -> list[float]:
        # Graphiti 呼叫 create(input_data=[text]) 取「單一」向量
        texts = [input_data] if isinstance(input_data, str) else list(input_data)
        vecs = await self._embed(texts)
        return vecs[0]

    async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
        return await self._embed(list(input_data_list))
