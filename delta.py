"""輸出圖譜的『差異』原始事實（給 n8n 的 LLM 節點寫簡報用）。

    python delta.py [--hours N]   → stdout 印 JSON：{hours, new_facts:[...], superseded:[...]}

分類邏輯（new_facts / superseded 怎麼判定）與 reporter.py 的 collect_delta
共用同一份實作，見 delta_core.classify_delta；本檔只回原始事實、不呼叫 LLM。
只需 falkordb client（不需 Graphiti），故 api.py 可直接 import compute_delta()。
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from falkordb import FalkorDB

from delta_core import classify_delta


def compute_delta(hours: int = 12) -> dict:
    """新增（created_at 落在視窗內）與被推翻（expired_at 落在視窗內）的事實。"""
    g = FalkorDB(
        host=os.getenv("FALKOR_HOST", "127.0.0.1"),
        port=int(os.getenv("FALKOR_PORT", "6379")),
    ).select_graph(os.getenv("FALKOR_DB", "news"))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    res = g.query(
        "MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity) "
        "RETURN a.name, b.name, r.fact, r.created_at, r.expired_at, r.valid_at, r.invalid_at"
    )
    # falkordb client 回傳的是 positional row，先轉成 dict 給共用的 classify_delta 用。
    rows = [
        {
            "src": a_name, "dst": b_name, "fact": fact,
            "created_at": created, "expired_at": expired,
            "valid_at": valid, "invalid_at": invalid,
        }
        for a_name, b_name, fact, created, expired, valid, invalid in (res.result_set or [])
    ]
    new_facts, superseded = classify_delta(rows, cutoff)
    return {"hours": hours, "new_facts": new_facts, "superseded": superseded}


def main():
    hours = 12
    a = sys.argv[1:]
    if "--hours" in a:
        hours = int(a[a.index("--hours") + 1])
    print(json.dumps(compute_delta(hours), ensure_ascii=False))


if __name__ == "__main__":
    main()
