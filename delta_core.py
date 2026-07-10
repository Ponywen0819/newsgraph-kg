"""delta 分類邏輯的共用核心（零重依賴，只用標準庫）。

`delta.py`（falkordb client，同步）與 `reporter.py`（Graphiti driver，非同步）
都需要把 RELATES_TO 邊依「created_at 落在視窗內 → 新增」「expired_at 落在視窗內
→ 被推翻」分類成 new_facts / superseded。這份邏輯只寫這一份，兩邊呼叫它，
避免語意漂移。

刻意不 import falkordb / graphiti_core / openai，好讓這個模組能在主機上
直接單元測試，不需要任何外部服務。
"""
from datetime import datetime, timezone


def parse_ts(v):
    """把各種時間表示（datetime / ISO 字串 / None / 空字串）轉成 aware datetime，失敗回 None。"""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def classify_delta(rows, cutoff):
    """把邊列表分類成 (new_facts, superseded)。

    rows：可迭代，每筆是 dict，需含 src/dst/fact/created_at/expired_at/valid_at/invalid_at 鍵。
    cutoff：aware datetime，視窗起點。

    分類規則（與 delta.py / reporter.py 原邏輯一致）：
    - fact 為空 → 跳過
    - expired_at 落在視窗內（>= cutoff）→ superseded（優先於 created 判斷）
    - 否則 created_at 落在視窗內（>= cutoff）→ new_facts
    - 其餘（含時間解析失敗）→ 不進任何清單
    """
    new_facts, superseded = [], []
    for r in rows:
        fact = r.get("fact")
        if not fact:
            continue
        rec = {
            "src": r.get("src"), "dst": r.get("dst"), "fact": fact,
            "valid_at": str(r.get("valid_at") or "")[:10],
            "invalid_at": str(r.get("invalid_at") or "")[:10],
        }
        created = parse_ts(r.get("created_at"))
        expired = parse_ts(r.get("expired_at"))
        if expired and expired >= cutoff:
            superseded.append(rec)
        elif created and created >= cutoff:
            new_facts.append(rec)
    return new_facts, superseded
