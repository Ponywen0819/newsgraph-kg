"""差異化簡報 reporter：讀圖譜「過去 N 小時的 delta」，寫一份只講新增與變化的簡報。

    python reporter.py                # 預設過去 24 小時，輸出 markdown 到 stdout
    python reporter.py --hours 48
    python reporter.py --json         # 輸出結構化 JSON（給 daily_digest / Discord 用）

核心概念：Graphiti 在 ingest 時已做完 entity/edge 去重，
所以「created_at 落在視窗內的邊」就是真正的新資訊（重複事實不會變成新邊）；
「expired_at 落在視窗內的邊」= 本來成立、今天被新事實推翻的舊事實 = 更新訊號。
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from .graph import build_graphiti, GROUP_ID
from .delta_core import classify_delta


async def collect_delta(graphiti, hours: int):
    """回傳 (new_facts, superseded_facts)。每筆含 fact / 兩端實體 / valid_at。

    分類規則（created_at 落在視窗內 → 新增；expired_at 落在視窗內 → 被推翻）
    與 delta.py 的 compute_delta 共用同一份實作，見 delta_core.classify_delta。
    """
    d = graphiti.driver
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    res = await d.execute_query(
        "MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity) "
        "RETURN a.name AS src, b.name AS dst, r.fact AS fact, "
        "r.created_at AS created_at, r.expired_at AS expired_at, "
        "r.valid_at AS valid_at, r.invalid_at AS invalid_at"
    )
    rows = res[0] if isinstance(res, tuple) else res
    return classify_delta(rows, cutoff)


def build_prompt(new_facts, superseded, hours):
    def fmt(items):
        lines = []
        for x in items:
            window = f"（{x['valid_at']}起）" if x["valid_at"] else ""
            lines.append(f"- [{x['src']} → {x['dst']}] {x['fact']} {window}".rstrip())
        return "\n".join(lines) if lines else "（無）"

    return (
        f"你是一位科技新聞編輯。以下是知識圖譜在過去 {hours} 小時新增與被推翻的『事實』"
        f"（已自動去重，重複的舊聞不會出現在這裡）。請寫一份給讀者的『差異化簡報』。\n\n"
        f"【今日新增的事實】\n{fmt(new_facts)}\n\n"
        f"【被更新／推翻的舊事實】\n{fmt(superseded)}\n\n"
        "務必『只輸出一個 JSON 物件』，不要任何其他文字或 markdown 框。結構：\n"
        "{\n"
        '  "headline": "一句話點出今天最重要的變化",\n'
        '  "whats_new": [ {"title":"新增重點標題", "detail":"2-3 句說明為何重要，可串連相關事實"} ],\n'
        '  "whats_changed": [ {"title":"變化重點", "detail":"講清楚 從什麼 → 變成什麼"} ],\n'
        '  "ongoing": "1-2 句帶過仍在進行中的主軸，不要重貼細節"\n'
        "}\n"
        "規則：whats_new 聚焦真正的新資訊，把相關的零碎事實合併成有意義的重點（別逐條照抄）；"
        "whats_changed 若無資料就給空陣列；用繁體中文、精簡有料。"
    )


async def write_report(new_facts, superseded, hours, as_json):
    from openai import AsyncOpenAI
    client = AsyncOpenAI(
        api_key=os.environ["DEEPSEEK_API_KEY"],
        base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
    )
    resp = await client.chat.completions.create(
        model=os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
        messages=[{"role": "user", "content": build_prompt(new_facts, superseded, hours)}],
        response_format={"type": "json_object"},
        temperature=0.3,
    )
    data = json.loads(resp.choices[0].message.content)
    if as_json:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return
    # markdown 呈現
    print(f"# 📊 差異化簡報（過去 {hours}h）\n")
    print(f"**{data.get('headline','')}**\n")
    if data.get("whats_new"):
        print("## 🆕 今日新增")
        for it in data["whats_new"]:
            print(f"- **{it.get('title','')}** — {it.get('detail','')}")
        print()
    if data.get("whats_changed"):
        print("## 🔄 有更新／被推翻")
        for it in data["whats_changed"]:
            print(f"- **{it.get('title','')}** — {it.get('detail','')}")
        print()
    if data.get("ongoing"):
        print(f"## 📌 進行中\n{data['ongoing']}")


async def run(graphiti, hours, as_json):
    try:
        new_facts, superseded = await collect_delta(graphiti, hours)
        print(f"[delta] 新增事實 {len(new_facts)} 筆、被推翻 {len(superseded)} 筆",
              file=sys.stderr)
        if not new_facts and not superseded:
            print("（過去視窗內沒有新增或變化）")
            return
        await write_report(new_facts, superseded, hours, as_json)
    finally:
        await graphiti.close()


def main():
    args = sys.argv[1:]
    hours = 24
    as_json = "--json" in args
    if "--hours" in args:
        hours = int(args[args.index("--hours") + 1])
    graphiti = build_graphiti()
    asyncio.run(run(graphiti, hours, as_json))


if __name__ == "__main__":
    main()
