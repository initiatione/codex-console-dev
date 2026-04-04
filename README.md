# codex-console

基于 [cnlimiter/codex-manager](https://github.com/cnlimiter/codex-manager) 持续修复和维护的增强版本。

这个版本的目标很直接：把近期 OpenAI 注册链路里那些“昨天还能跑，今天突然翻车”的坑补上，让注册、登录、拿 token、上传、任务调度、支付相关能力和打包运行都更稳一点。

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Version](https://img.shields.io/badge/version-v1.1.2-2563eb.svg)](https://github.com/dou-jiang/codex-console/releases/tag/v1.1.2)

## 项目地址

- GitHub Repo: [https://github.com/dou-jiang/codex-console](https://github.com/dou-jiang/codex-console)
- Blog: [https://blog.cysq8.cn/](https://blog.cysq8.cn/)

## QQ群

- 交流群: [291638849（点击加群）](https://qm.qq.com/q/4TETC3mWco)
- Telegram 频道: [codex_console](https://t.me/codex_console)

## Blog 说明

我会在 Blog 持续更新这些内容：

- 部署教程和环境配置说明
- 每个版本的更新日志和 Release 说明
- 常见报错、排查思路和修复记录
- 邮箱服务、上传服务、任务调度、自检功能等使用说明
- 上游变化后的兼容性调整说明

访问地址：

- [https://blog.cysq8.cn/](https://blog.cysq8.cn/)

## 赞助支持

如果这个项目对你有帮助，欢迎赞助支持项目继续维护与更新。

<table>
  <tr>
    <td align="center">
      <strong>微信赞助</strong><br />
      <img src="docs/assets/wechat-pay.png" alt="微信赞助二维码" width="260" />
    </td>
    <td align="center">
      <strong>支付宝赞助</strong><br />
      <img src="docs/assets/alipay-pay.png" alt="支付宝赞助二维码" width="260" />
    </td>
  </tr>
</table>

## 致谢

首先感谢上游项目作者 [cnlimiter](https://github.com/cnlimiter) 提供的优秀基础工程。

本仓库是在原项目思路和结构之上进行兼容性修复、流程调整和体验优化，适合作为一个“当前可用的修复维护版”继续使用。

## 开源共享说明

- 仓库前面的项目地址、Blog、社群、赞助与致谢信息会继续保留，并作为 README 的重点信息长期展示。
- 本仓库定位仍然是基于上游持续修复、持续维护的开源共享版本，方便直接使用、排障和二次开发。
- 如果你是通过第三方付费获取本项目，建议优先回到仓库与 Blog 核对原始说明，避免被二次倒卖或拿过时版本。

## 版本更新

### v1.0

1. 新增 Sentinel POW 求解逻辑。  
   OpenAI 现在会强制校验 Sentinel POW，原先直接传空值已经不行了，这里补上了实际求解流程。

2. 注册和登录拆成两段。  
   现在注册完成后通常不会直接返回可用 token，而是跳转到绑定手机或后续页面。  
   本分支改成“先注册成功，再单独走一次登录流程拿 token”，避免卡死在旧逻辑里。

3. 去掉重复发送验证码。  
   登录流程里服务端本身会自动发送验证码邮件，旧逻辑再手动发一次，容易让新旧验证码打架。  
   现在改成直接等待系统自动发来的那封验证码邮件。

4. 修复重新登录流程的页面判断问题。  
   针对重新登录时页面流转变化，调整了登录入口和密码提交逻辑，减少卡在错误页面的情况。

5. 优化终端和 Web UI 提示文案。  
   保留可读性的前提下，把一些提示改得更友好一点，出错时至少不至于像在挨骂。

### v1.1

1. 修复注册流程中的问题，解决 Outlook 和临时邮箱收不到邮件导致注册卡住、无法完成注册的问题。

2. 修复无法检查订阅状态的问题，提升订阅识别和状态检查的可用性。

3. 新增绑卡半自动模式，支持自动随机地址；3DS 无法跳过，需按实际流程完成验证。

4. 新增已订阅账号管理功能，支持查看和管理账号额度。

5. 新增后台日志功能，并补充数据导出与导入能力，方便排查问题和迁移数据。

6. 优化部分 UI 细节与交互体验，减少页面操作时的割裂感。

7. 补充细节稳定性处理，尽量减少注册、订阅检测和账号管理过程中出现卡住或误判的情况。

### v1.1.1

1. 新增 `CloudMail` 邮箱服务实现，并完成服务注册、配置接入、邮件轮询、验证码提取和基础收件处理能力。

2. 新增上传目标 `newApi` 支持，可根据配置选择不同导入目标类型。

3. 新增 `Codex` 账号导出格式，支持后续登录、迁移和导入使用。

4. 新增 `CPA` 认证文件 `proxy_url` 支持，现可在 CPA 服务配置中保存和使用代理地址。

5. 优化 OAuth token 刷新兼容逻辑，完善异常返回与一次性令牌场景处理，降低刷新报错概率。

6. 优化批量验证流程，改为受控并发执行，减少长时间阻塞和卡死问题。

7. 修复模板渲染兼容问题，提升不同 Starlette 版本下页面渲染稳定性。

8. 修复六位数字误判为 OTP 的问题，避免邮箱域名或无关文本中的六位数字被错误识别为验证码。

9. 新增 Outlook 账户“注册状态”识别与展示功能，可直接看到“已注册 / 未注册”，并支持显示关联账号编号。

10. 修复 Outlook 邮箱匹配大小写问题，避免 Outlook.com 因大小写差异被误判为未注册。

11. 修复 Outlook 列表列错位、乱码和占位文案问题，恢复中文显示并优化列表信息布局。

12. 优化 WebUI 端口冲突处理，默认端口占用时自动切换可用端口。

13. 增加启动时轻量字段迁移逻辑，自动补齐新增字段，提升旧数据升级兼容性。

14. 批量注册上限由 `100` 提升至 `1000`，前后端同步。

15. 公告区改为固定文案与固定链接，强化“永久免费开源、禁止倒卖、付费请退款”提示，并新增爱发电支持入口。

### v1.1.2

1. 新增统一鉴权与安全基线。  
   增加 `src/web/auth.py`、首次改密页面 `setup_password.html`，统一 API 与 WebSocket 鉴权口径，并补齐默认密码改密流程。

2. 新增系统自检能力与修复中心。  
   增加 `system_selfcheck.py`、`selfcheck.py`、`selfcheck.html`、`selfcheck_scheduler.py`，支持自检记录、调度与修复动作。

3. 新增统一任务中心。  
   增加 `tasks.py`，统一管理注册、支付、自检、Auto Team 等任务的状态、配额、暂停、继续、取消和重试。

4. 新增 Auto Team 模块。  
   增加后端 `auto_team.py` 与前端 `auto_team.js`、`auto_team_manage.js`。

5. 新增数据库迁移体系。  
   引入 Alembic，补齐 `alembic.ini`、`alembic/` 目录与迁移说明。

6. 新增母子标签系统和卡池功能。  
   补齐账号标签、池状态、自动绑卡与上游对接能力。

7. 扩展邮箱服务。  
   保留原有邮箱链路，同时补齐 `CloudMail`、`LuckMail`、`YYDS Mail` 等服务接入。

8. 新增周期任务调度。  
   支持计划任务创建、编辑、启停、立即执行以及前端轮询管理。

9. 新增 New-API 服务上传。  
   支持独立服务配置、测试、单账号上传、批量上传和注册成功后自动上传。

10. 增强自动注册与自动补货链路。  
    增加自动注册核心模块、库存监控、补货计划生成、取消感知、批次统计修复，以及 PR60 anyauto V2 回退流程。

11. 优化前端请求与轮询稳定性。  
    对 `app.js`、`accounts.js`、`payment.js`、`utils.js` 等进行了去重、降噪、超时、并发与降级路径增强。

12. 增强注册链路容错。  
    `src/core/register.py` 增加登录续跑、continue URL 缓存、workspace 缓存、OTP 重试等处理，对 native / abcard / outlook 等入口做了细分。

13. 增强账号模型与业务语义。  
    增加 `role_tag`、`biz_tag`、`pool_state`、`priority`、`last_used_at` 等字段，支持团队池、候选池、阻断池等状态表达。

14. 增强支付与历史数据保留。  
    `BindCardTask` 支持账号删除后保留 `account_email` 快照；新增审计日志模型 `OperationAuditLog` 与 `SelfCheckRun` 记录。

15. 扩展配置能力。  
    `settings.py` 增加自检、熔断、自动刷新、自动注册等多项配置，支持更多运行时调参。

16. 补充测试与 CI。  
    新增多组测试：任务、安全、自注册、新 API、自动刷新、邮箱服务等，并新增 `.github/workflows/tests.yml`。

17. 修复自动一键刷新、公告样式与页面外链。  
    修复定时自动一键刷新不可用、公告按钮样式问题、Footer 外链和 Blog 跳转问题。

## 核心能力

- Web UI 管理注册任务、账号、支付、自检、邮箱服务、卡池、Auto Team 和日志数据
- 支持单任务、批量任务、自动补货、计划任务、任务暂停 / 继续 / 取消 / 重试
- 支持多种邮箱服务接码和自部署邮箱接入
- 支持 CPA、Sub2API、Team Manager、New-API 等上传链路
- 支持 SQLite 和远程 PostgreSQL
- 支持打包为 Windows / Linux / macOS 可执行文件
- 更适配当前 OpenAI 注册与登录链路

## 部署说明

- 完整部署说明已整理到：[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)
- 如果你只想快速启动，本地推荐直接看 `python start_webui.py` 这一套；容器部署则优先看 `docker compose up -d`。

## 环境要求

- Python 3.10+
- `uv`（推荐）或 `pip`
- 如需使用推荐启动器 / Docker Compose，请先安装 Docker

## 安装依赖

```bash
# 使用 uv（推荐）
uv sync

# 或使用 pip
pip install -r requirements.txt
```

## 快速开始

### 1）本地快速体验（SQLite）

```bash
python webui.py
```

- 默认访问：`http://127.0.0.1:8000`
- 默认数据库：`data/database.db`
- 适合单机测试、快速体验

### 2）Windows / 本地开发推荐方式

```bash
python start_webui.py
```

- 推荐组合：`Docker PostgreSQL + 本机 python webui.py`
- 启动器会优先检查 `.env` 中的 PostgreSQL 配置
- 数据库不可达时会自动尝试 `docker compose up -d postgres`
- 会自动选择可用端口并在服务可访问后打开浏览器

常用参数：

- `python start_webui.py --dry-run`
- `python start_webui.py --skip-docker`
- `python start_webui.py --no-browser`
- `python start_webui.py --port 8080`

### 3）Docker Compose 部署

```bash
docker compose up -d
```

默认入口：

- Web UI：`http://127.0.0.1:1455`
- noVNC：`http://127.0.0.1:6080`
- PostgreSQL：`127.0.0.1:5432`

> 注意：本地 `python webui.py` 默认端口是 `8000`，`docker compose` 默认端口是 `1455`。

## 常用环境变量

可选。复制 `.env.example` 为 `.env` 后按需修改：

```bash
cp .env.example .env
```

常用变量：

| 变量 | 说明 | 默认值 |
| --- | --- | --- |
| `APP_HOST` | Web UI 监听主机 | `0.0.0.0` |
| `APP_PORT` | Web UI 监听端口 | `8000` |
| `APP_ACCESS_PASSWORD` | Web UI 访问密钥 | `admin123` |
| `APP_DATABASE_URL` | 数据库连接字符串 | `postgresql://codex_user:codex_pass@postgres:5432/codex` |
| `APP_DB_POOL_SIZE` | 连接池常驻连接数 | `20` |
| `APP_DB_MAX_OVERFLOW` | 额外连接数 | `20` |
| `APP_DB_POOL_TIMEOUT` | 等待连接超时（秒） | `30` |
| `APP_DB_POOL_RECYCLE` | 连接回收时间（秒） | `1800` |
| `APP_DB_POOL_USE_LIFO` | 是否优先复用最近归还连接 | `true` |

优先级：`命令行参数 > 环境变量(.env) > 数据库设置 > 默认值`

## 部署 / 迁移 / 备份说明

为避免 README 重复过长，详细内容已集中到这些文档：

- 完整部署说明：[`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md)
- Alembic 迁移说明：[`alembic/README.md`](alembic/README.md)
- SQLite 迁移到 PostgreSQL：`python scripts/migrate_sqlite_to_postgres.py --source data/database.db --target <postgresql_url>`

`docs/DEPLOYMENT.md` 已整理这些内容：

- 本地 SQLite 启动
- `start_webui.py` 推荐启动方式
- 外部 PostgreSQL 部署
- Docker Compose / Docker run
- 端口与持久化目录约定
- 备份与恢复步骤
- 常见部署误区

## 打包为可执行文件

```bash
# Windows
build.bat

# Linux/macOS
bash build.sh
```

Windows 打包完成后，默认会在 `dist/` 目录生成类似下面的文件：

```text
dist/codex-console-windows-X64.exe
```

如果打包失败，优先检查：

- Python 是否已加入 PATH
- 依赖是否安装完整
- 杀毒软件是否拦截了 PyInstaller 产物
- 终端里是否有更具体的报错日志

## 项目定位

这个仓库更适合作为：

- 原项目的修复增强版
- 当前注册链路的兼容维护版
- 自己二次开发的基础版本

如果你准备公开发布，建议在仓库描述里明确写上：

`Forked and fixed from cnlimiter/codex-manager`

这样既方便别人理解来源，也对上游作者更尊重。

## 仓库命名

当前仓库名：

`codex-console`

## 安全基线说明（新增）

- `/api/*` 与 `/api/ws/*` 已统一接入登录鉴权
- 首次启动检测到默认口令或默认密钥时，会强制跳转到 `/setup-password` 完成改密
- 支付相关 API Key 不再使用代码内硬编码默认值，需通过环境变量或配置显式提供

## 数据库迁移（Alembic）

```bash
alembic revision --autogenerate -m "your_change"
alembic upgrade head
```

初始化与更多说明见：

- `alembic/README.md`

Alembic 现在默认让 `alembic.ini` 中的 `sqlalchemy.url` 保持为空，再按 `APP_DATABASE_URL`、`DATABASE_URL`、`src.config.settings.get_database_url()` 的顺序解析目标数据库，所以不需要手动切回 SQLite。

## 免责声明

本项目仅供学习、研究和技术交流使用，请遵守相关平台和服务条款，不要用于违规、滥用或非法用途。

因使用本项目产生的任何风险和后果，由使用者自行承担。
