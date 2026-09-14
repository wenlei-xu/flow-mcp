# gflow studio

## 定位

这是 Flow2API 操作后台的 gflow-cli 适配版实现。页面使用 React/Vite，保留熟悉的后台工作流，但把底层对象从 `Token/ST/AT` 改为：

```text
Chrome profile → Flow project → generation task → local asset
```

## 视觉与交互原则

- 视觉主题：深墨绿色控制台 + 浅纸面工作区，强调“本地控制平面”而不是云端 SaaS。
- 内容结构：概览 → 账号管理 → 请求日志 → 测试页面。
- 交互重点：账号生命周期、请求排障和单次真实测试，保持 Flow2API 后台的核心管理闭环。
- 动效：页面切换淡入上移、状态标签变化、提交后任务进入队列并即时反馈。

## 页面设计

### 概览

显示启用账号、排队/运行中、已完成和失败数量，以及最近请求和当前账号状态。

### 账号管理

- 新增 Profile 并启动 Chrome 登录
- 查看 Google 账号、登录状态、额度和最近使用时间
- 管理每个 Profile 的 Flow 项目池、启用状态和默认项目
- 编辑 Profile 名称和备注
- 设置默认账号
- 启用/禁用账号
- 重新登录、验证和删除 Profile

禁用状态持久化在 `gflow-studio.db` 中；禁用账号不能进入新的生成任务。

账号并发采用 Profile 安全模型：每个持久化 Chrome Profile 的图片和视频有效并发
均为 1，多个 Profile 之间可以并行。Profile API 会返回
`image_concurrency`/`video_concurrency` 以及 `effective_*_concurrency` 和当前
in-flight 数量；全局 Worker 数不会绕过 ProfileLease，也不会把同一 Google 会话
强行驱动成多个并发浏览器。若需要提高总并发，应增加已登录账号，而不是复制同一
Profile 的并发任务。

gflow-cli MCP 另外有一个进程级的生成安全桶：默认允许 8 个生成请求突发，之后每
20 秒补充 1 个令牌。可通过 `GFLOW_CLI_GENERATION_RATE_CAPACITY` 和
`GFLOW_CLI_GENERATION_RATE_REFILL_SECONDS` 调整；它只控制本地提交节奏，不会增加
Google 配额，也不会把单个 Chrome Profile 变成多并发会话。
工作台的“系统设置”也可修改这两个值；修改会写入 `studio_settings`，Worker 重启
后会重新加载，避免只改了 API 进程而 Worker 仍使用旧值。

### 请求日志

- 展示排队、运行中、成功、失败和需核对的请求
- 合并 gflow-studio 任务与 gflow-cli 失败记录，使用稳定日志 ID
- 按状态筛选
- 打开完整日志详情
- 清除本地工作台日志（不删除 gflow-cli 原始数据）

### 测试页面

支持图片/视频、模型、画幅/时长、T2V/I2V/R2V、Flow 项目和参考图上传。普通测试默认使用“自动选择（账号池）”，也可以指定单个账号排障。上传接口先落地到适配层，生成请求不携带大段 Base64；提交后自动轮询并展示状态、结果文件和原始响应。


## 后端适配层

React 源码位于 `react-app/src`，构建产物位于 `dist`；FastAPI 根路由优先提供 `dist/index.html`，因此旧版单文件 HTML 不再是运行入口。

前端不直接接触 Chrome profile，也不把登录凭据放进浏览器 localStorage。当前使用同机 Web API：

```text
Web UI
  → gflow-studio API
    → gflow MCP / local Python adapter
      → gflow-cli profile lease
        → real Chrome + Flow
```

实际 API 契约：

| 页面能力 | 适配 API |
| --- | --- |
| profiles | `GET /api/profiles`、`POST /api/profiles`、`PUT /api/profiles/{name}`、`DELETE /api/profiles/{name}`、`POST /api/profiles/{name}/login`、`GET /api/profiles/{name}/status` |
| credits | `GET /api/profiles/{name}/credits` |
| projects | `GET /api/projects?profile=NAME`、`POST /api/projects`；账号项目池使用 `GET/POST/PUT/DELETE /api/profiles/{name}/projects` |
| assets | `POST /api/assets`、`POST /api/assets/import`、`GET /api/assets`、`POST /api/assets/cleanup` |
| generation | `POST /api/generations`、`POST /api/generations/batch`、`GET /api/generations/{id}`、`POST /api/generations/{id}/cancel`、`POST /api/generations/{id}/retry` |
| test / generation | `POST /api/generations`（`profile` 省略或为 `null` 时自动走账号池）、`GET /api/generations/{id}` |
| logs / media | `GET /api/logs`（支持 status/q/since/until/offset）、`GET /api/logs/{id}`、`GET /api/logs/export?format=csv`、`DELETE /api/logs`、签名结果链接 |
| health / stats / models | `GET /api/health`、`GET /api/stats`、`GET /api/models`、`GET/PUT /api/config` |

统一外部入口：

```text
MCP Streamable HTTP: /mcp
OpenAI models:       /v1/models
OpenAI Responses:    /v1/responses
OpenAI images:       /v1/images/generations
OpenAI image edits:  /v1/images/edits (multipart image/file → asset_id)
Video extension:     /v1/videos/generations, /v1/videos/{id}, /v1/generations/{id}
Video control:        /v1/videos/generations/{id}/cancel|retry
Chat compatibility:  /v1/chat/completions
```

生成任务使用 SQLite 持久化队列。提交时可带 `Idempotency-Key` 或
`idempotency_key`；相同请求会返回原任务，不会重复扣费。任务启动后会受单任务超时
保护，服务重启会恢复 queued 任务，running/cancelling 任务会变为 interrupted，必须
人工确认后重试。参考图先通过 `/api/assets` 上传，再使用 `input_asset_ids`，不会把
Base64 塞进生成请求。

当 gflow-cli 上游发出 `submit_attempted` 或 `remote_started` checkpoint 时，适配层
会同步写入任务的 `progress`、`phase` 和受限的上游句柄，并通过任务查询/SSE 暴露；
没有 checkpoint 时不会伪造精确百分比，只保留已知阶段。

生产运行建议拆成三个进程：`gflow serve` 提供底层 gflow MCP，`uvicorn app:app` 只提供
后台 API，`python worker.py` 独立消费 SQLite 队列。API 服务使用
`GFLOW_STUDIO_ROLE=api`，Worker 使用 `GFLOW_STUDIO_ROLE=worker`；Uvicorn 必须保持单
Worker，避免多个进程同时驱动同一个 Chrome Profile。对应的 systemd 单元和 Caddy 配置
位于 `deploy/`。

## 与现有 Flow2API 的边界

可以复用页面的布局、样式、列表和预览交互；不能直接复用 Token 管理、ST/AT 刷新、Token 轮询和旧的 `/v1/chat/completions` 实现。当前实现已连接真实 gflow-cli 适配层，但必须先在本机通过 Chrome 完成 profile 登录后才能实际生成。

生成任务在提交前会对候选 Profile 做真实的 Flow 会话探测；鉴权失败的 Profile 会持久化标记为 `invalid` 并自动移出账号池，网络暂时不可用则标记为 `unavailable` 并在冷却后重新探测。健康状态可通过 `GET /api/profiles/{name}/status` 刷新。

自动生成时优先使用 Profile 的默认项目，没有默认项目时按使用次数选择启用项目；首次发现 Flow 项目会导入本地项目池。项目池只保存引用和调度元数据，不会删除 Flow 远端项目。

默认 API 只监听 `127.0.0.1`，不会把 Chrome profile、Cookie 或任务凭据直接暴露给网页。公网部署必须配置 `.env` 中的 API Key/管理员密码和 `GFLOW_STUDIO_PUBLIC_BASE_URL`，通过 `deploy/Caddyfile.example` 反代 HTTPS；不要直接暴露 8090 或 18080。
