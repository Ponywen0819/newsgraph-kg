"""把圖裡的 Episodic 還原成 ingest 用的 articles JSON，寫進 state volume 備份。"""
import asyncio, json
from graph import build_graphiti

async def run(g):
    d = g.driver
    try:
        res = await d.execute_query(
            "MATCH (n:Episodic) RETURN n.name AS name, n.content AS content, "
            "n.source_description AS src, n.valid_at AS valid_at "
            "ORDER BY n.valid_at"
        )
        rows = res[0] if isinstance(res, tuple) else res
        arts = []
        for r in rows:
            content = r.get("content") or ""
            parts = content.split("\n\n", 1)
            title = (parts[0] or r.get("name") or "").strip()
            body = (parts[1].strip() if len(parts) > 1 else "")
            src = (r.get("src") or "").strip()
            i = src.find("http")
            if i >= 0:
                source, url = src[:i].strip(), src[i:].strip()
            else:
                source, url = src, ""
            arts.append({
                "id": url or title,
                "title": title,
                "body": body,
                "url": url,
                "source": source or "unknown",
                "published_at": str(r.get("valid_at") or ""),
            })
        out = "/app/state/backup_articles.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(arts, f, ensure_ascii=False, indent=2)
        print(f"已備份 {len(arts)} 篇 → {out}")
    finally:
        await g.close()

g = build_graphiti(); asyncio.run(run(g))
