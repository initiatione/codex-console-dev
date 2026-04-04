# 当前部署说明

本文把当前仓库的部署方式按 **本地部署** 和 **服务器部署** 两类重新整理，方便直接按场景阅读，不必在 `README.md`、`docker-compose.yml`、`.env.example`、`start_webui.py` 之间来回翻。

## 1. 先看结论：怎么选

| 场景 | 推荐方式 | 说明 |
| --- | --- | --- |
| 本地快速体验 | `python webui.py` + SQLite | 最轻量，适合单机测试 |
| Windows 本地开发 | `python start_webui.py` + Docker PostgreSQL | 当前最省心，本机跑 WebUI、Docker 跑库 |
| 服务器正式运行 | `python webui.py` + 外部 PostgreSQL | 适合独立数据库、便于备份和扩展 |
| 容器化运行 | `docker compose up -d` | 适合把 WebUI、PostgreSQL、noVNC 一起带起来 |

## 2. 当前项目里的关键部署文件

- `README.md`：总说明与快速入口
- `docs/DEPLOYMENT.md`：当前这份部署说明
- `start_webui.py`：Windows / 本地开发推荐启动器
- `webui.py`：Web UI 直接启动入口
- `docker-compose.yml`：容器化部署入口，内置 `postgres` + `webui`
- `Dockerfile`：单镜像构建定义
- `.env.example`：环境变量示例
- `scripts/migrate_sqlite_to_postgres.py`：SQLite 迁移到 PostgreSQL 脚本
- `alembic/README.md`：数据库迁移说明

## 3. 通用约定

### 3.1 环境要求

- Python `3.10+`
- 推荐使用 `uv`，也支持 `pip`
- 如需容器部署：Docker Desktop / Docker Engine + Docker Compose
- 如需容器内可视化浏览器：浏览器可访问 `6080`

安装依赖：

```bash
# 推荐
uv sync

# 或
pip install -r requirements.txt
```

> `requirements.txt` 已包含 `playwright`；Docker 镜像构建时还会执行 `python -m playwright install --with-deps chromium`。

### 3.2 目录与持久化

- `data/`：应用数据目录；默认 SQLite 数据库也在这里
- `logs/`：运行日志目录；主日志通常是 `logs/app.log`
- `pgdata/`：`docker compose` 下 PostgreSQL 的宿主机持久化目录
- `backups/`：建议自行创建，用于放数据库备份文件

### 3.3 端口约定

| 场景 | 默认端口 | 说明 |
| --- | --- | --- |
| `python webui.py` | `8000` | 本地直接启动默认监听端口 |
| `python start_webui.py` | `8000` | 若被占用，会自动顺延到下一个可用端口 |
| `docker compose` 中的 Web UI | `1455` | 容器默认映射为 `1455:1455` |
| noVNC | `6080` | 容器内可视化浏览器入口 |
| PostgreSQL（compose） | `5432` | 默认映射到宿主机 `127.0.0.1:5432` |

> 最容易混淆的一点：**本地 Python 直启默认是 `8000`，Docker Compose 默认是 `1455`。**

### 3.4 配置优先级

当前运行时可以统一理解为：

- Web UI 相关：`命令行参数 > 环境变量 > 数据库设置 > 默认值`
- 数据库连接：优先看 `APP_DATABASE_URL`，其次 `DATABASE_URL`，再回退到 SQLite

常用环境变量：

| 变量 | 用途 |
| --- | --- |
| `APP_DATABASE_URL` | 推荐使用的数据库连接字符串 |
| `DATABASE_URL` | 兼容变量名，优先级低于 `APP_DATABASE_URL` |
| `APP_HOST` / `WEBUI_HOST` | Web UI 监听地址 |
| `APP_PORT` / `WEBUI_PORT` | Web UI 监听端口 |
| `APP_ACCESS_PASSWORD` / `WEBUI_ACCESS_PASSWORD` | Web UI 访问密码 |
| `APP_DB_POOL_*` | PostgreSQL / MySQL 连接池参数 |
| `DEBUG` | 调试模式 |
| `LOG_LEVEL` | 日志等级 |

---

## 4. 本地部署

适合个人使用、本机调试、开发环境和本地排障。

### 4.1 方案 A：本地快速体验（SQLite）

如果只是快速体验或单机测试，可以直接用 SQLite：

```bash
python webui.py
```

默认行为：

- Web UI 默认监听 `0.0.0.0:8000`
- SQLite 数据库默认落在 `data/database.db`
- 首次启动会自动初始化表结构和默认设置

也可以显式指定：

```bash
python webui.py --host 127.0.0.1 --port 8000
python webui.py --debug
python webui.py --access-password your_password
```

适用建议：

- 本地快速验证页面和接口
- 不需要独立数据库
- 并发不高、无需复杂备份策略

### 4.2 方案 B：Windows / 本地开发推荐方式

这是当前仓库最省心的本地开发方式：**Docker 跑 PostgreSQL，本机 Python 跑 Web UI。**

执行：

```bash
python start_webui.py
```

启动器会自动做这些事情：

- 读取 `.env` 里的 `APP_DATABASE_URL` / `DATABASE_URL`
- 如果目标 PostgreSQL 已可直连，则直接复用
- 如果目标 PostgreSQL 不可达，则尝试 `docker compose up -d postgres`
- 等待数据库就绪
- 用当前 Python 进程启动 `webui.py`
- 自动选择可用端口
- Web UI 可访问后自动打开浏览器

常用参数：

```bash
python start_webui.py --dry-run
python start_webui.py --skip-docker
python start_webui.py --no-browser
python start_webui.py --port 8080
python start_webui.py --host 0.0.0.0 --port 8080
```

适用建议：

- 本机已经安装 Docker Desktop
- 希望数据库单独持久化在 `pgdata/`
- 想保留 Python 直调试能力

### 4.3 本地 `.env` 建议

可选。复制 `.env.example` 为 `.env` 后按需修改：

```bash
cp .env.example .env
```

本地最常用的是这几项：

```env
APP_HOST=0.0.0.0
APP_PORT=8000
APP_ACCESS_PASSWORD=your_password
APP_DATABASE_URL=postgresql://codex_user:codex_pass@127.0.0.1:5432/codex
```

如果你本地就是 SQLite，也可以不配 `APP_DATABASE_URL`，程序会自动回退到 `data/database.db`。

### 4.4 本地常见问题

#### 为什么 `python webui.py` 是 8000，但 Docker 里是 1455？

- `webui.py` 本地默认端口是 `8000`
- `docker-compose.yml` 里显式配置的是 `1455`

#### 为什么 `start_webui.py` 没帮我拉起 PostgreSQL？

因为启动器会先检查 `.env` 里的 PostgreSQL 地址是否已经可直连；如果能连上，就直接复用，不会重复拉 Docker。

#### 为什么改了数据库配置，还是连旧库？

优先检查：

1. 是否设置了 `APP_DATABASE_URL`
2. 是否同时存在 `DATABASE_URL`
3. 是否数据库里已经保存过设置
4. 是否本次启动用了命令行参数覆盖

---

## 5. 服务器部署

适合长期运行、独立数据库、对外服务或容器化部署。

### 5.1 方案 A：Python + 外部 PostgreSQL

适合你已经有独立 PostgreSQL 实例，或希望数据库与应用分离的情况。

先配置环境变量：

```bash
export APP_DATABASE_URL="postgresql://user:password@host:5432/dbname"
export APP_DB_POOL_SIZE=20
export APP_DB_MAX_OVERFLOW=20
export APP_DB_POOL_TIMEOUT=30
export APP_DB_POOL_RECYCLE=1800
export APP_DB_POOL_USE_LIFO=true
```

再执行：

```bash
alembic upgrade head
python webui.py --host 0.0.0.0 --port 8000
```

补充说明：

- PostgreSQL URL 在运行时会自动转成 `postgresql+psycopg://...`
- 如果你把 `.env` 放在项目根目录，`webui.py` 会自动读取
- 若从 SQLite 切换到 PostgreSQL，历史数据不会自动过去，需要手动迁移

迁移命令：

```bash
python scripts/migrate_sqlite_to_postgres.py --source data/database.db --target postgresql://codex_user:codex_pass@127.0.0.1:5432/codex
```

### 5.2 方案 B：Docker Compose 一把起

执行：

```bash
docker compose up -d
```

当前 compose 默认包含：

- `postgres`：`postgres:16-alpine`
- `webui`：由本仓库 `Dockerfile` 构建
- `webui` 依赖 `postgres` 健康检查
- Web UI 容器内会顺带启动 Xvfb、fluxbox、x11vnc、noVNC

默认访问入口：

- Web UI：`http://127.0.0.1:1455`
- noVNC：`http://127.0.0.1:6080`
- PostgreSQL：`127.0.0.1:5432`

默认持久化：

- `./data:/app/data`
- `./logs:/app/logs`
- `./pgdata:/var/lib/postgresql/data`

compose 中已注入的关键变量：

- `WEBUI_HOST=0.0.0.0`
- `WEBUI_PORT=1455`
- `APP_DATABASE_URL=postgresql://codex_user:codex_pass@postgres:5432/codex`
- `APP_DB_POOL_SIZE=20`
- `APP_DB_MAX_OVERFLOW=20`
- `APP_DB_POOL_TIMEOUT=30`
- `APP_DB_POOL_RECYCLE=1800`
- `APP_DB_POOL_USE_LIFO=true`
- `WEBUI_ACCESS_PASSWORD=admin123`

适用建议：

- 你想快速起完整运行环境
- 你需要 noVNC 看容器内浏览器
- 你希望 Web UI 与 PostgreSQL 都跟着容器走

### 5.3 方案 C：单独跑 Docker 镜像

如果你不想直接用 `docker compose`，也可以自己 `docker run`：

```bash
docker run -d \
  -p 1455:1455 \
  -p 6080:6080 \
  -e DISPLAY=:99 \
  -e ENABLE_VNC=1 \
  -e VNC_PORT=5900 \
  -e NOVNC_PORT=6080 \
  -e WEBUI_HOST=0.0.0.0 \
  -e WEBUI_PORT=1455 \
  -e WEBUI_ACCESS_PASSWORD=your_secure_password \
  -e APP_DATABASE_URL=postgresql://codex_user:codex_pass@host.docker.internal:5432/codex \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  --name codex-console \
  <your-image>:latest
```

> 如果容器外没有单独数据库，这种方式还得自己额外准备 PostgreSQL；大多数情况下还是更推荐 `docker compose up -d`。

### 5.4 服务器安全基线

当前仓库已有基础安全逻辑：

- `/api/*` 与 `/api/ws/*` 统一接入登录鉴权
- 如果仍使用默认访问密码，首次会被引导到 `/setup-password`
- 支付相关 API Key 需要显式配置，不再依赖代码内默认值

服务器部署时至少建议做这几件事：

1. 修改默认访问密码
2. 明确监听地址，并配合反向代理或防火墙
3. 生产环境优先用 PostgreSQL，不建议长期用 SQLite
4. 对 `data/`、`logs/`、`pgdata/` 做持久化和备份

---

## 6. 迁移、备份与恢复

### 6.1 Alembic 数据库迁移

```bash
alembic revision --autogenerate -m "your_change"
alembic upgrade head
```

初始化与更多说明见：

- `alembic/README.md`

Alembic 默认让 `alembic.ini` 中的 `sqlalchemy.url` 保持为空，再按 `APP_DATABASE_URL`、`DATABASE_URL`、`src.config.settings.get_database_url()` 的顺序解析目标数据库。

### 6.2 PostgreSQL 备份（推荐）

备份：

```bash
docker compose up -d postgres
mkdir backups
docker compose exec postgres sh -c "pg_dump -U codex_user -d codex -Fc -f /tmp/codex.dump"
docker cp <postgres-container>:/tmp/codex.dump backups/codex.dump
```

恢复：

```bash
docker cp backups/codex.dump <postgres-container>:/tmp/codex.dump
docker compose exec postgres sh -c "pg_restore -U codex_user -d codex --clean --if-exists /tmp/codex.dump"
```

### 6.3 SQLite 备份

如果还在使用 SQLite，至少备份：

- `data/database.db`
- `data/` 下其他业务数据
- `logs/`（用于排障）

### 6.4 冷备份提醒

如果你确实要直接复制 `pgdata/` 做冷备份，请先停止 `postgres` 容器，再整体复制该目录；不要在 PostgreSQL 运行中直接拷目录做热备份。

---

## 7. 部署后如何自检

最少检查这几项：

1. Web UI 是否能打开
2. `logs/app.log` 是否持续写入
3. 数据库是否可连接
4. 首次访问是否进入登录或改密流程
5. 如使用容器可视化，`http://127.0.0.1:6080` 是否可访问

## 8. 相关文档入口

- 总说明：`README.md`
- 数据库迁移：`alembic/README.md`
- SQLite 迁移脚本：`scripts/migrate_sqlite_to_postgres.py`
