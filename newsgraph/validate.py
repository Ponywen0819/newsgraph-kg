"""接線驗證：在 event loop 外建構 Graphiti（避免 driver 自動背景建索引造成並發衝突）。"""
import asyncio
from .graph import build_graphiti

async def run(g):
    await g.build_indices_and_constraints()
    print("build_indices_and_constraints() OK：FalkorDB 索引已建（無連線衝突）")
    await g.close()
    print("close() OK")

def main():
    g = build_graphiti()  # 同步階段建構 → 無 running loop → driver 不會自動排背景 task
    print("build_graphiti() OK")
    asyncio.run(run(g))

main()
