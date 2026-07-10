"""統一的唯讀圖存取層。viz 端點與 delta.compute_delta 共用,消除 _parse_ts/
_split_src/裸 falkordb 查詢的重複。只用 falkordb(唯讀)+ 標準庫,不扛整個 Graphiti。
falkordb 延遲 import,讓本模組能在無 falkordb 的主機上被 import 測試。"""
from .config import FALKOR_HOST, FALKOR_PORT, FALKOR_DB
from .delta_core import parse_ts  # 重用同一份時間解析


def graph():
    from falkordb import FalkorDB  # 延遲 import
    return FalkorDB(host=FALKOR_HOST, port=FALKOR_PORT).select_graph(FALKOR_DB)


def rows(cypher, params=None):
    return graph().query(cypher, params or {}).result_set or []


def split_src(sd):
    """source_description = '來源名 https://...' → (source, url)。"""
    sd = (sd or "").strip()
    i = sd.find("http")
    if i >= 0:
        return sd[:i].strip(), sd[i:].strip()
    return sd, ""


def ep_meta(uuid, name, sd, valid_at):
    source, url = split_src(sd)
    return {"uuid": uuid, "name": name, "source": source, "url": url,
            "valid_at": (str(valid_at)[:10] if valid_at else None)}
