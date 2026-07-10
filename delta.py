"""輸出圖譜的『差異』原始事實（給 n8n 的 LLM 節點寫簡報用）。

    python delta.py [--hours N]   → stdout 印 JSON：{hours, new_facts:[...], superseded:[...]}

與 reporter.py 的 collect_delta 同邏輯，但只回原始事實、不呼叫 LLM。
只需 falkordb client（不需 Graphiti），故 api.py 可直接 import compute_delta()。
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from falkordb import FalkorDB


def _parse_ts(v):
    if v is None or v == "":
        return None
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


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
    new_facts, superseded = [], []
    for row in (res.result_set or []):
        a_name, b_name, fact, created, expired, valid, invalid = row
        if not fact:
            continue
        rec = {"src": a_name, "dst": b_name, "fact": fact,
               "valid_at": str(valid or "")[:10], "invalid_at": str(invalid or "")[:10]}
        c, e = _parse_ts(created), _parse_ts(expired)
        if e and e >= cutoff:
            superseded.append(rec)
        elif c and c >= cutoff:
            new_facts.append(rec)
    return {"hours": hours, "new_facts": new_facts, "superseded": superseded}


def main():
    hours = 12
    a = sys.argv[1:]
    if "--hours" in a:
        hours = int(a[a.index("--hours") + 1])
    print(json.dumps(compute_delta(hours), ensure_ascii=False))


if __name__ == "__main__":
    main()
