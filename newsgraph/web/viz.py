"""新聞知識圖譜視覺化服務（v1：唯讀 + 時間軸）。

用 FalkorDB client 直查圖，把 Entity-Entity 的事實邊轉成 Cytoscape 格式；
支援「as of 某時間點」過濾（只顯示該時刻成立的邊）與實體關鍵字過濾。

env：FALKOR_HOST/FALKOR_PORT/FALKOR_DB、VIZ_HOST/VIZ_PORT、RECENT_DAYS。
"""
import os
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from ..graphrepo import rows, split_src, ep_meta, parse_ts

RECENT_DAYS = float(os.getenv("RECENT_DAYS", "1"))
HERE = os.path.dirname(os.path.abspath(__file__))

app = FastAPI(title="NewsGraph Viz")


@app.get("/api/stats")
def stats():
    def one(q):
        r = rows(q)
        return r[0][0] if r else 0
    return {
        "entities": one("MATCH (n:Entity) RETURN count(n)"),
        "episodes": one("MATCH (n:Episodic) RETURN count(n)"),
        "relations": one("MATCH ()-[r:RELATES_TO]->() RETURN count(r)"),
    }


@app.get("/api/timespan")
def timespan():
    """時間軸範圍＝新聞（Episodic）本身的觀測窗，而非事實 valid_at 的極值。
    這樣不會被『2018 舊事實 / 未來生效日』等離群事實把滑桿撐爆、把近期新聞擠成一點。
    """
    rs = rows(
        "MATCH (n:Episodic) WHERE n.valid_at IS NOT NULL RETURN n.valid_at"
    )
    ts = sorted(t for t in (parse_ts(r[0]) for r in rs) if t)
    if not ts:
        now = datetime.now(timezone.utc)
        return {"min": (now - timedelta(days=7)).isoformat(), "max": now.isoformat()}
    lo, hi = ts[0], ts[-1]
    if hi - lo < timedelta(hours=12):   # 只有一天資料 → 左右各墊半天，滑桿才拉得動
        lo, hi = lo - timedelta(hours=12), hi + timedelta(hours=12)
    return {"min": lo.isoformat(), "max": hi.isoformat()}


@app.get("/api/graph")
def graph(
    as_of: str | None = Query(None, description="ISO 時間點；只顯示該時刻成立的邊"),
    entity: str | None = Query(None, description="實體名稱關鍵字（子字串、不分大小寫）"),
    limit: int = Query(400, ge=1, le=2000),
):
    t = parse_ts(as_of) or datetime.now(timezone.utc)
    recent_cut = t - timedelta(days=RECENT_DAYS)
    kw = (entity or "").strip().lower()

    # 用 reference_time（新聞觀測日）當時間軸：只顯示「到 as_of 為止、從新聞學到」的事實，
    # 於是拖滑桿＝看圖隨每天新聞累積長大。valid_at（事件日）另外回傳供詳情顯示。
    # 加上 ORDER BY：截斷（超過 limit）時，優先保留「最近學到」的邊，避免每次回傳不確定的任意子集
    rs = rows(
        "MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity) "
        "RETURN a.name, b.name, r.fact, r.valid_at, r.reference_time "
        "ORDER BY r.reference_time DESC"
    )

    nodes, edges, deg = {}, [], {}
    for a_name, b_name, fact, valid_at, ref_time in rs:
        if not a_name or not b_name:
            continue
        ref = parse_ts(ref_time)
        va = parse_ts(valid_at)
        if ref and ref > t:           # 這條是 as_of 之後才從新聞學到 → 還不該出現
            continue
        if kw and kw not in a_name.lower() and kw not in b_name.lower():
            continue
        recent = bool(ref and ref >= recent_cut)   # 近 RECENT_DAYS 天新學到 → 標綠
        edges.append({"data": {
            "id": f"e{len(edges)}", "source": a_name, "target": b_name,
            "label": fact or "", "recent": recent,
            "valid_at": va.date().isoformat() if va else None,
            "learned_at": ref.date().isoformat() if ref else None,
        }})
        for n in (a_name, b_name):
            nodes.setdefault(n, {"data": {"id": n, "label": n}})
            deg[n] = deg.get(n, 0) + 1
        if len(edges) >= limit:
            break

    for n, nd in nodes.items():
        nd["data"]["degree"] = deg.get(n, 0)
    return {
        "as_of": t.isoformat(),
        "elements": {"nodes": list(nodes.values()), "edges": edges},
        "counts": {"nodes": len(nodes), "edges": len(edges), "truncated": len(edges) >= limit},
    }


@app.get("/api/episodes")
def episodes(q: str | None = Query(None), limit: int = Query(200, ge=1, le=1000)):
    """所有來源新聞清單（可用關鍵字過濾標題），依時間新到舊。"""
    rs = rows(
        "MATCH (n:Episodic) RETURN n.uuid, n.name, n.source_description, n.valid_at"
    )
    kw = (q or "").strip().lower()
    items = [ep_meta(*r) for r in rs if r[1]]
    if kw:
        items = [it for it in items if kw in (it["name"] or "").lower()
                 or kw in (it["source"] or "").lower()]
    items.sort(key=lambda it: it["valid_at"] or "", reverse=True)
    return {"count": len(items), "items": items[:limit]}


@app.get("/api/episode/{uuid}")
def episode(uuid: str):
    """單篇新聞全文 + 它提到的實體。"""
    rs = rows(
        "MATCH (n:Episodic {uuid:$u}) "
        "RETURN n.uuid, n.name, n.content, n.source_description, n.valid_at",
        {"u": uuid},
    )
    if not rs:
        return {"error": "not found"}
    u, name, content, sd, valid_at = rs[0]
    meta = ep_meta(u, name, sd, valid_at)
    ments = rows(
        "MATCH (n:Episodic {uuid:$u})-[:MENTIONS]->(e:Entity) RETURN e.name",
        {"u": uuid},
    )
    meta["content"] = content or ""
    meta["mentions"] = sorted({r[0] for r in ments if r[0]})
    return meta


@app.get("/api/entity_episodes")
def entity_episodes(name: str = Query(...), limit: int = Query(50, ge=1, le=500)):
    """某實體被哪些新聞提到（走 MENTIONS）。"""
    rs = rows(
        "MATCH (ep:Episodic)-[:MENTIONS]->(e:Entity {name:$n}) "
        "RETURN ep.uuid, ep.name, ep.source_description, ep.valid_at",
        {"n": name},
    )
    items = [ep_meta(*r) for r in rs if r[1]]
    items.sort(key=lambda it: it["valid_at"] or "", reverse=True)
    return {"count": len(items), "items": items[:limit]}


@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "static", "index.html"))


app.mount("/static", StaticFiles(directory=os.path.join(HERE, "static")), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("VIZ_HOST", "127.0.0.1"),
                port=int(os.getenv("VIZ_PORT", "8088")))
