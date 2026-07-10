# 執行報告:合併雙服務為單一統一服務

**狀態**:進行中(P1)　|　**分支**:`refactor/unify-service`　|　**開始**:2026-07-11

---

## 1. 目標

把目前**兩個服務 + 三個進入點**收斂成**單一 FastAPI 服務、單一映像、單一 Quadlet 單元**,並統一程式架構(套件化、統一讀取層、統一 Web 框架)。

## 2. 現況(重構前)

| | newsgraph-api | newsgraph-viz | CLI |
|---|---|---|---|
| Quadlet | `newsgraph-api.container` | `newsgraph-viz.container` | 無(run.sh) |
| 映像 | `newsgraph:latest`(重,graphiti) | `newsgraph-viz:latest`(輕,fastapi) | `newsgraph:latest` |
| Web 框架 | stdlib `http.server`+執行緒 | FastAPI/uvicorn | — |
| 埠 | 8090 | 8088 | — |
| 交付 | mount `/app:ro` | COPY 進 image | mount |
| 驗證 | X-API-Key | 無 | — |

**痛點**:兩映像/兩框架/兩埠/兩交付;讀取層(`_parse_ts`/`_split_src`/裸 falkordb 查詢)在 viz 與 delta 重複。

## 3. 目標架構(重構後)

```
n8n ─POST/queue(X-API-Key)─▶ ┌──────────────────────────────────────────┐
瀏覽器 ─HTTP(讀取免驗證)────▶ │ newsgraph :8080  單一 FastAPI 服務          │
                            │  寫入 API(驗證)  ┃  讀取/視覺化(公開)       │
                            │  /queue/push …   ┃  /api/graph … /static    │
                            │  lifespan async worker(單一 Graphiti,序列)│
                            │            └─────── graphrepo(統一讀取層)  │
                            └───────────────────┬──────────────────────────┘
                            單一映像 · 單一 Quadlet │ falkordb :6379
```

## 4. 已鎖定決策

1. **目錄結構** = 完整 Python 套件 `newsgraph/`(web/ clients/ graphrepo/ queue/ 分層)。
2. **程式碼交付** = COPY 進 image(可重現/正式)。
3. **CLI 統一** = 這輪**不做**;各模組保留 `main()`,改以 `python -m newsgraph.<模組>` 呼叫。
4. **埠** = 8080(取代 8090+8088)。

## 5. 目標目錄

```
newsgraph-kg/
├── Containerfile              # 單一映像:全依賴 + COPY 套件與 static
├── run.sh                     # python -m newsgraph.ingest / .reporter …
├── deploy/newsgraph.container # 單一 Quadlet(取代 api + viz)
└── newsgraph/
    ├── config.py              # 集中 env
    ├── graph.py               # build_graphiti(worker 延遲載入)
    ├── clients/{deepseek,nvidia_embedder}.py
    ├── graphrepo.py           # ★ 統一唯讀圖存取(falkordb + parse_ts + split_src + 查詢)
    ├── delta_core.py · delta.py · reporter.py · ingest.py
    ├── query.py · diag.py · dump.py · validate.py   # 各自 main()
    ├── queue.py               # ★ 佇列 + lifespan async worker
    └── web/{app,routes_ingest,routes_view}.py + static/index.html
```

## 6. 分階段計畫與狀態

| 階段 | 內容 | 風險 | 狀態 |
|---|---|---|---|
| **P1** | 套件骨架、搬模組進 `newsgraph/`、抽 `graphrepo.py`、`config.py`、delta 與 viz 讀取改走 graphrepo、run.sh 改 `-m` | 低(純重構) | ✅ 完成(commit `c7645bd`) |
| **P2** | 新 FastAPI `web/app.py`(併 viz 路由 + 移植 api 端點)、drainer→lifespan async worker、寫入路由加 API-Key、單一埠 | **高** | ✅ 完成(`a467169`+修復 `bea002a`) |
| **P3** | 單一 Containerfile(COPY)、`deploy/newsgraph.container`、退役 viz 映像與兩舊單元 | 中 | ✅ 完成(`8ef7ea8`);兩舊 Quadlet 於 P5 停用 |
| **P5** | 容器 smoke test(canary 起 :8080 不動舊服務)、切換 n8n 8090→8080 與 ssh 8088→8080、停舊單元 flip Quadlet | 中 | ⏳ 待辦(需 build image + FalkorDB + 使用者確認) |

**P2 複核修正**:`_aclose_graphiti` 原在 `asyncio.run` 專屬 loop 掃 `all_tasks()`;移到共享 uvicorn loop 後會空等 uvicorn 自身 task 到 30s 逾時,已移除該掃描(`bea002a`)。
**P5 待驗地雷**:`supervisor` 於 uvicorn loop 內呼叫 `build_graphiti()`,FalkorDriver 會自動排背景建索引 task(與舊 api.py 同,非回歸),smoke test 要確認首次 ingest 不噴「Connection closed」。

> 本分支在全部階段完成、容器 smoke test 通過前**不 merge 進 main**,避免半遷移狀態影響每日 07:00 生產管線。

## 7. 最高風險:drainer → lifespan async worker

新 worker 為 uvicorn 事件圈內的長駐 task,靠 `asyncio.Event` 被 `/queue/push` 喚醒,持有單一 Graphiti 實例序列灌圖。**必須保住 PR#4 三保證**:
- **崩潰不遺失**:peek 佇列首篇 → ingest → 成功/失敗後才 pop 寫檔(at-least-once,靠 seen.txt 去重)。
- **不永久卡死**:worker 常駐、不依賴 `_draining` 旗標;task 異常結束由 app 層節流重建。
- **全失敗仍觸發 webhook**:以 `attempted>0` 判斷,payload 帶 `{processed, failed, attempted}`。

PR#4 的四項 mock 測試將原封移植到新 worker 當回歸護欄。

## 8. 切換(P5,漏改會斷管線)

- n8n HTTP 節點 `host.containers.internal:8090/queue/push` → `:8080`。
- 個人 `ssh -L 8088:127.0.0.1:8088` → `8080`。
- `done_webhook`(指向 n8n 自身)不受影響。

## 9. 回滾

本分支未 merge 前,生產仍跑舊的 `newsgraph-api` + `newsgraph-viz`。切換後若異常,`systemctl --user` 停用 `newsgraph.container`、重新啟用兩舊單元即可退回。
