# NewsGraph — 新聞時序知識圖譜

用 **Graphiti**(增量式時序知識圖譜)+ **FalkorDB**(Redis-based 圖資料庫)把每天的新聞收進一張會「隨時間長大」的知識圖譜,支援時間回溯、關聯探索,並自動產出「只講新增與變化」的差異化簡報。

- **抽取 LLM**:DeepSeek(OpenAI 相容 API)
- **Embedding**:NVIDIA NIM `nv-embed-v1`(4096 維)
- **存儲**:FalkorDB(ARM 友善、省記憶體)
- **視覺化**:FastAPI + Cytoscape.js 的唯讀圖譜瀏覽 + 時間軸

## 運作概念

新聞 → **Graphiti**(自動 entity/關係去重、雙時間軸 `valid_at`/`invalid_at` 追蹤變化)→ **reporter**(讀最近 N 小時的 delta,寫差異化簡報:重複的舊聞不再重貼,只講新增與被推翻的事實)。

Graphiti 在灌圖時已完成去重,因此:
- `created_at` 落在時間窗內的邊 = 真正的新資訊
- `expired_at` 落在時間窗內的邊 = 被新事實推翻的舊事實

## 檔案結構

| 檔案 | 用途 |
|---|---|
| `graph.py` | 建立設定好的 Graphiti 實例(DeepSeek + NVIDIA embedder + FalkorDB) |
| `ingest.py` | 把一批新聞 JSON 灌進圖譜(含繁中語言正規化) |
| `reporter.py` | 讀圖譜 delta,用 LLM 寫差異化簡報 |
| `delta.py` | 只回傳原始 delta 事實(不呼叫 LLM),供外部服務取用 |
| `api.py` | 薄 HTTP API:佇列式序列灌圖 + `/delta`(給自動化流程呼叫) |
| `query.py` / `diag.py` / `dump.py` | 語意查詢 / 品質診斷 / 備份還原 |
| `nvidia_embedder.py` | NVIDIA NIM 的自訂 embedder |
| `deepseek_client.py` | DeepSeek 的 LLM client(處理其 json_object 回傳的攤平) |
| `viz/` | 唯讀圖譜視覺化服務(FastAPI + Cytoscape.js) |

## 快速開始

```bash
# 1) 設定金鑰
cp .env.example .env      # 填入 DEEPSEEK_API_KEY、NVIDIA_API_KEY、NEWSGRAPH_API_KEY

# 2) 啟動 FalkorDB(範例)
podman run -d --name falkordb -p 127.0.0.1:6379:6379 \
  -v falkordb-data:/var/lib/falkordb/data falkordb/falkordb:latest

# 3) 建 image
podman build -t localhost/newsgraph:latest .

# 4) 灌一批範例新聞
cat sample_news.json | ./run.sh ingest -

# 5) 產出差異化簡報
./run.sh reporter --hours 24

# 6) 查詢
./run.sh query search "台積電 最近的動向"
./run.sh query stats
```

## 新聞輸入格式

`ingest` 吃一個 JSON 陣列,每篇物件欄位(只有 `title` 必填):

```json
{
  "id": "唯一識別(建議用網址)",
  "title": "新聞標題",
  "body": "內文或摘要",
  "url": "https://...",
  "source": "媒體名",
  "published_at": "2026-06-30T08:00:00+08:00"
}
```

範例見 `sample_news.json`。
