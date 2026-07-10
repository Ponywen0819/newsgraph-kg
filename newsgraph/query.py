"""查詢知識圖譜。

    python query.py search "郭台銘 最近的動向"     # 語意搜尋關係（會用到時間版本資訊）
    python query.py stats                          # 節點/邊統計
"""
import asyncio
import sys

from .graph import build_graphiti, GROUP_ID


async def do_search(graphiti, query: str) -> None:
    try:
        edges = await graphiti.search(query, group_ids=[GROUP_ID], num_results=15)
        if not edges:
            print("（沒有結果）")
            return
        for e in edges:
            valid = getattr(e, "valid_at", None)
            invalid = getattr(e, "invalid_at", None)
            window = f"  [valid {valid} → {invalid or '至今'}]" if valid else ""
            print(f"• {e.fact}{window}")
    finally:
        await graphiti.close()


async def do_stats(graphiti) -> None:
    try:
        driver = graphiti.driver
        for label, q in [
            ("Episodes (新聞)", "MATCH (n:Episodic) RETURN count(n) AS c"),
            ("Entities (實體)", "MATCH (n:Entity) RETURN count(n) AS c"),
            ("Relations (關係邊)", "MATCH ()-[r:RELATES_TO]->() RETURN count(r) AS c"),
        ]:
            try:
                res = await driver.execute_query(q)
                rows = res[0] if isinstance(res, tuple) else res
                print(f"{label}: {rows}")
            except Exception as ex:
                print(f"{label}: 查詢失敗 {ex}")
    finally:
        await graphiti.close()


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        return
    cmd = sys.argv[1]
    graphiti = build_graphiti()  # 在 event loop 外建構
    if cmd == "search" and len(sys.argv) > 2:
        asyncio.run(do_search(graphiti, " ".join(sys.argv[2:])))
    elif cmd == "stats":
        asyncio.run(do_stats(graphiti))
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
