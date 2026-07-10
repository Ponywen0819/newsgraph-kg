"""把一批新聞餵進知識圖譜。

用法：
    python ingest.py articles.json      # 從檔案讀
    cat articles.json | python ingest.py -   # 從 stdin 讀

輸入 = 一個 JSON 陣列，每篇新聞是一個物件（這就是 collector 要產出的格式）：
    {
      "id":           "唯一識別（建議用網址或新聞站的文章ID）",   # 去重用，缺省時用 url 或 title 雜湊
      "title":        "新聞標題",                              # 必填
      "body":         "內文或摘要",                            # 建議；越完整抽出的圖越好
      "url":          "https://...",                          # 來源連結（溯源）
      "source":       "中央社",                               # 媒體名
      "published_at": "2026-06-30T08:00:00+08:00"             # ISO8601；缺省時用現在時間
    }
"""
import asyncio
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from graphiti_core.nodes import EpisodeType
from graph import build_graphiti, GROUP_ID

TZ = ZoneInfo("Asia/Taipei")
# 已處理過的新聞 id（避免重複 ingest）；放在掛載的 state volume
SEEN_FILE = Path("/app/state/seen.txt")

# 語言正規化：把任何來源語言統一成台灣繁體中文，改善跨語言 entity 去重。
# 混合策略（兼顧成本/安全）：
#   - 中文為主的文字 → 只過 OpenCC s2twp（即時、免費、確定性；簡→台灣繁，含詞彙如 网络→網路）
#   - 非中文為主（英文/日文等，中文<門檻） → 先用 LLM 翻成繁中（嚴格保留專有名詞/數字/事實），再過 OpenCC 保證繁體
# 開關：NORMALIZE_LLM=0 → 只用 OpenCC；OPENCC_CONFIG 可覆寫（如 s2t 只轉字不轉詞）；
#       NORMALIZE_CJK_RATIO 可調「視為需翻譯」的中文佔比門檻（預設 0.20）。
try:
    from opencc import OpenCC

    _cc = OpenCC(os.getenv("OPENCC_CONFIG", "s2twp"))

    def _to_trad(text: str) -> str:
        return _cc.convert(text) if text else text
except Exception as _e:  # 套件缺失/設定錯誤 → 退化成不轉換，至少不讓 ingest 壞掉
    print(f"[warn] OpenCC 不可用，跳過繁體轉換：{_e}", file=sys.stderr)

    def _to_trad(text: str) -> str:
        return text

_NORMALIZE_LLM = os.getenv("NORMALIZE_LLM", "1") != "0"
_CJK_RATIO = float(os.getenv("NORMALIZE_CJK_RATIO", "0.20"))
_CJK_RE = re.compile(r"[一-鿿]")           # 中日韓漢字（不含假名）
_KANA_HANGUL_RE = re.compile(r"[぀-ヿ가-힣]")  # 日文假名 / 韓文諺文 → 一定是外語


def _needs_translation(text: str) -> bool:
    """判斷是否為外語來源、需要翻成繁中。
    - 含假名/諺文 → 日/韓文，直接要翻。
    - 否則看漢字佔比：低於門檻（預設 20%）視為英文等外語為主。
    這樣「中文為主、只夾英文專有名詞」的常見標題不會被誤翻。
    """
    stripped = re.sub(r"\s", "", text)
    if not stripped:
        return False
    if _KANA_HANGUL_RE.search(stripped):
        return True
    return len(_CJK_RE.findall(stripped)) / len(stripped) < _CJK_RATIO


_translate_client = None


def _translate_to_trad(text: str) -> str:
    """用 DeepSeek 把外語新聞忠實翻成繁中；失敗則退回原文（外層仍會過 OpenCC）。"""
    global _translate_client
    try:
        from openai import OpenAI
        if _translate_client is None:
            _translate_client = OpenAI(
                api_key=os.environ["DEEPSEEK_API_KEY"],
                base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"),
            )
        resp = _translate_client.chat.completions.create(
            model=os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
            messages=[
                {"role": "system", "content": (
                    "你是專業新聞翻譯。無論輸入是英文、日文、韓文或任何語言，"
                    "一律輸出『台灣繁體中文』譯文；即使只有一句也要翻譯，絕不原樣照抄非中文內容。"
                )},
                {"role": "user", "content": (
                    "把以下新聞文字忠實翻成台灣繁體中文。規則："
                    "①公司/產品/模型/人名等專有名詞保留原文（如 Nvidia、OpenAI、GPT-5.6、SK하이닉스→SK海力士這類有通用譯名者用通用譯名）；"
                    "②數字、百分比、金額、日期完全保留；③不要摘要、不要增刪事實、逐句翻；"
                    "④只輸出翻譯結果，不要任何說明或引號。\n\n" + text
                )},
            ],
            temperature=0,
        )
        out = (resp.choices[0].message.content or "").strip()
        return out or text
    except Exception as e:
        print(f"[warn] LLM 翻譯失敗，退回原文+OpenCC：{e}", file=sys.stderr)
        return text


def normalize_zh(text: str) -> str:
    if not text:
        return text
    if _NORMALIZE_LLM and _needs_translation(text):
        text = _translate_to_trad(text)
    return _to_trad(text)  # 一律過 OpenCC 收尾，保證台灣繁體


def load_articles(arg: str) -> list[dict]:
    raw = sys.stdin.read() if arg == "-" else Path(arg).read_text(encoding="utf-8")
    data = json.loads(raw)
    if isinstance(data, dict):  # 容忍 {"articles": [...]} 包裝
        data = data.get("articles", [])
    return data


def article_id(a: dict) -> str:
    if a.get("id"):
        return str(a["id"])
    basis = a.get("url") or a.get("title", "")
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()


def parse_time(a: dict) -> datetime:
    raw = a.get("published_at")
    if raw:
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=TZ)
        except ValueError:
            pass
    return datetime.now(TZ)


def load_seen() -> set[str]:
    if SEEN_FILE.exists():
        return set(SEEN_FILE.read_text(encoding="utf-8").split())
    return set()


def mark_seen(ids: list[str]) -> None:
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SEEN_FILE.open("a", encoding="utf-8") as f:
        for i in ids:
            f.write(i + "\n")


async def run(graphiti, articles: list[dict], close: bool = True) -> None:
    seen = load_seen()
    # 索引/約束只需在此 graphiti 實例上建一次；api.py 的 drainer 會共用同一實例逐篇呼叫 run()，
    # 用旗標掛在實例上避免每篇都多打一輪 build_indices_and_constraints round-trip。
    if not getattr(graphiti, "_ng_indices_built", False):
        await graphiti.build_indices_and_constraints()  # 冪等，安全重複呼叫
        graphiti._ng_indices_built = True

    done: list[str] = []
    try:
        for idx, a in enumerate(articles, 1):
            aid = article_id(a)
            if aid in seen:
                print(f"[{idx}/{len(articles)}] skip（已處理）: {a.get('title','')[:40]}")
                continue
            title = normalize_zh((a.get("title") or "").strip())
            if not title:
                print(f"[{idx}/{len(articles)}] skip（無標題）")
                continue
            # 確定要處理這篇了：立刻標記為 seen，避免同一批 payload 內出現重複 id 時被重複灌入
            # （跨批的去重靠 SEEN_FILE 持久化，失敗重試靠 done 只在成功後才寫入 mark_seen，不受此影響）
            seen.add(aid)
            try:
                body = normalize_zh((a.get("body") or "").strip())
                episode_body = f"{title}\n\n{body}".strip()
                source = a.get("source") or "unknown"
                url = a.get("url") or ""
                ref_time = parse_time(a)
                print(f"[{idx}/{len(articles)}] ingest: {title[:50]} @ {ref_time.date()}")
                await graphiti.add_episode(
                    name=title[:120],
                    episode_body=episode_body,
                    source_description=f"{source} {url}".strip(),
                    reference_time=ref_time,
                    source=EpisodeType.text,
                    group_id=GROUP_ID,
                    # 之後要「詳細設定 entity」就在這裡掛 entity_types / edge_types
                )
                done.append(aid)
            except Exception as e:
                # 單篇失敗（如 DeepSeek structured output 偶發出包）不該中斷整批
                print(f"[{idx}/{len(articles)}] 失敗跳過：{str(e)[:160]}", file=sys.stderr)
                continue
        if done:
            mark_seen(done)
        print(f"\n完成：新增 {len(done)} 篇，略過 {len(articles) - len(done)} 篇。")
    finally:
        # 由呼叫端管理生命週期時（如 api.py 的排空迴圈共用一個實例）不在這裡關閉
        if close:
            await graphiti.close()


def main() -> None:
    arg = sys.argv[1] if len(sys.argv) > 1 else "-"
    articles = load_articles(arg)
    graphiti = build_graphiti()  # 在 event loop 外建構，避免 driver 自動背景建索引衝突
    asyncio.run(run(graphiti, articles))


if __name__ == "__main__":
    main()
