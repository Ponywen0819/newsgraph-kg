"""圖譜品質診斷：時間分布 / 實體去重 / 熱門實體時序。"""
import asyncio
import sys
from collections import defaultdict

from .graph import build_graphiti, GROUP_ID


async def q(driver, cypher):
    res = await driver.execute_query(cypher)
    return res[0] if isinstance(res, tuple) else res


async def run(graphiti):
    d = graphiti.driver
    try:
        # 1) 每天進了幾篇（用 episode 的 valid_at / created_at）
        print("=== 1) 時間分布（每天新增 Episodes）===")
        rows = await q(d, "MATCH (n:Episodic) RETURN n.valid_at AS t")
        by_day = defaultdict(int)
        for r in rows:
            t = str(r.get("t") or "")[:10]
            by_day[t] += 1
        for day in sorted(by_day):
            print(f"  {day}: {by_day[day]} 篇")

        # 2) 熱門實體（依關係邊數排序），順便肉眼看有沒有同義沒合併
        print("\n=== 2) 熱門實體 Top 25（name — 連接邊數）===")
        rows = await q(d, (
            "MATCH (e:Entity)-[r:RELATES_TO]-() "
            "RETURN e.name AS name, count(r) AS deg "
            "ORDER BY deg DESC LIMIT 25"
        ))
        for r in rows:
            print(f"  {r.get('deg'):>3}  {r.get('name')}")

        # 3) 全部實體名稱（找疑似重複：繁簡、代號、大小寫、子字串）
        print("\n=== 3) 疑似重複實體（名稱互為子字串或去空白後相同）===")
        rows = await q(d, "MATCH (e:Entity) RETURN e.name AS name")
        names = sorted({r.get("name") for r in rows if r.get("name")})
        flagged = set()
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                na, nb = a.replace(" ", "").lower(), b.replace(" ", "").lower()
                if na and nb and (na in nb or nb in na):
                    flagged.add((a, b))
        if flagged:
            for a, b in sorted(flagged):
                print(f"  ⚠ {a!r}  ~  {b!r}")
        else:
            print("  （沒有明顯的子字串重複）")
        print(f"\n  實體總數 {len(names)}")
    finally:
        await graphiti.close()


def main():
    graphiti = build_graphiti()
    asyncio.run(run(graphiti))


if __name__ == "__main__":
    main()
