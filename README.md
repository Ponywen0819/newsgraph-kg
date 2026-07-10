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

## 架構

**單一 FastAPI 服務**(埠 8080):寫入 API(需 `X-API-Key`)+ 唯讀視覺化(公開)共用同一 app;灌圖由 lifespan 內的常駐 async worker 序列處理。整個專案是一個 Python 套件 `newsgraph/`。

| 路徑 | 用途 |
|---|---|
| `newsgraph/graph.py` | 建立設定好的 Graphiti 實例(DeepSeek + NVIDIA embedder + FalkorDB) |
| `newsgraph/ingest.py` | 把一批新聞 JSON 灌進圖譜(含繁中語言正規化) |
| `newsgraph/reporter.py` | 讀圖譜 delta,用 LLM 寫差異化簡報 |
| `newsgraph/delta.py` · `delta_core.py` | 回傳原始 delta 事實(不呼叫 LLM);分類核心零依賴、可單測 |
| `newsgraph/graphrepo.py` | 統一唯讀圖存取層(falkordb 直查,viz 與 delta 共用) |
| `newsgraph/config.py` | 集中環境變數設定 |
| `newsgraph/queue.py` | 持久佇列 + lifespan 常駐 async worker(序列灌圖、崩潰不遺失) |
| `newsgraph/web/` | 統一 FastAPI app(`app.py` + `routes_ingest.py` 寫入 + `routes_view.py` 視覺化 + `static/`) |
| `newsgraph/clients/` | DeepSeek client / NVIDIA embedder |
| `newsgraph/query.py` · `diag.py` · `dump.py` | 語意查詢 / 品質診斷 / 備份還原(`python -m newsgraph.<模組>`) |
| `deploy/newsgraph.container` | podman Quadlet 部署單元 |

## 快速開始

```bash
# 1) 設定金鑰
cp .env.example .env      # 填入 DEEPSEEK_API_KEY、NVIDIA_API_KEY、NEWSGRAPH_API_KEY

# 2) 啟動 FalkorDB(範例)
podman run -d --name falkordb -p 127.0.0.1:6379:6379 \
  -v falkordb-data:/var/lib/falkordb/data falkordb/falkordb:latest

# 3) 建 image(單一映像)
podman build -t localhost/newsgraph:latest .

# 4) 啟動統一服務(寫入 API + 視覺化,單一埠 8080)
cp deploy/newsgraph.container ~/.config/containers/systemd/
systemctl --user daemon-reload && systemctl --user start newsgraph
#   → 視覺化:http://<host>:8080/　寫入 API:POST http://<host>:8080/queue/push(需 X-API-Key)

# 5) 灌一批範例新聞(CLI)
cat sample_news.json | ./run.sh ingest -

# 6) 產出差異化簡報 / 查詢(CLI)
./run.sh reporter --hours 24
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
