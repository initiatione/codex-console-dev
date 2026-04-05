# Changelog

## v1.1.3

- 补充 LuckMail 复用已购邮箱阶段的显式诊断日志：当当前条件下没有任何可复用候选时，日志会直接说明“未找到可复用已购邮箱候选，跳过邮箱可用性检查并直接尝试新购”，避免误以为系统没有执行可用性检查；并新增对应回归测试。

- 修复本机 Codex 新会话的 Windows shell 环境缺项：全局 `C:\\Users\\liuchf\\.codex\\config.toml` 的 `shell_environment_policy.set` 已补入 `PATHEXT`，与已有的 `SystemRoot` / `windir` / `ComSpec` 一起确保新会话默认即可正常拉起 `python.exe`、`cmd.exe`、`git.exe` 等可执行程序。

- 优化 LuckMail 当前实跑链路的探活速度：Rust CLI 的 `check_token_alive` 现支持单次请求级超时透传，服务层新增 `token_alive_request_timeout` 与 `reuse_purchase_candidate_limit`，复用已购邮箱默认只快速探测少量候选；同时补齐邮箱服务配置接口与回归测试，新增 `fallback_to_order_on_no_stock` 暴露，便于排查“无库存未回退 order”这类运行时配置问题。

- 修复当前手动运行时的 LuckMail / 网络双环境问题：为实际运行的 `D:\\MiniConda\\python.exe` 安装 `luckmail-sdk` 并升级 `curl_cffi` 到 `0.15.0`，恢复 Rust 失败后的 Python fallback；同时为 IP 地理位置检查补充标准库网络兜底，并把 LuckMail 的 SDK 缺失提示与 `rust_cli_path` 占位文案改为系统安装路径语义。

- 修复 LuckMail 收不到验证码的问题：LuckMail 服务现优先加载仓库内最新 SDK 资源，并在复用/新购已购邮箱后先做 token alive 可用性检查；若 get_token_code 未返回验证码，还会回退到邮件列表/详情接口提取验证码，减少失活邮箱与单接口漏码导致的超时失败。

- 补充 LuckMail 配置兼容与异常兜底：邮箱服务配置接口现暴露 SDK 优先级、收件模式、探活与回退等关键参数；同时在 `get_token_code` 接口异常时，也会继续尝试邮件列表/详情接口，降低单接口抖动导致的漏码概率。

- 预埋 LuckMail Rust CLI 双后端能力：新增本地 Rust CLI backend 封装、`sdk_preference`/`rust_cli_path` 配置与 Python fallback 选择逻辑；同时在 `LuckMailSdk-Rust` 中补入 `luckmail-cli` 二进制入口源码，待本机 Rust toolchain 恢复后即可编译接入。

- 优化注册任务启动响应构造：单任务/普通批量启动现会在数据库会话仍存活时生成响应快照并回填已选 `email_service_id`，减少 detached ORM 对象往返与批量启动后的重复查询，响应字段更稳定。

- 修复注册任务响应序列化回归：`task_to_response()` 现对 detached 的 SQLAlchemy `RegistrationTask` 实例做安全快照，避免在 Session 已关闭时访问 `email_service` 懒加载关系而触发 `DetachedInstanceError`；并补充对应回归测试。

- 重组 `docs/DEPLOYMENT.md` 结构：按“本地部署 / 服务器部署”两大场景重新编排，方便按用途快速查阅，并保留原有迁移、备份、自检与端口约定。

- 整理当前部署说明：新增 `docs/DEPLOYMENT.md`，集中说明本地 SQLite、本地启动器、外部 PostgreSQL、Docker Compose、端口约定、持久化目录、备份与迁移步骤，并在 `README.md` 增加文档入口。

- 修复邮箱服务删除链路：删除服务时会先清理其关联的非运行注册记录；其中 pending 待执行任务会先标记取消再清理，只有 running 中的注册任务才会阻止删除邮箱服务。

- 优化邮箱服务删除确认弹窗：前端删除前会先查询关联影响，明确显示将清理的注册记录数量；若仍存在执行中任务，会提前提示并阻止误删。

- 修复自动注册后调度灯不亮的问题：注册流程里自动上传到 Codex2API / Sub2API / Team Manager / NewAPI 成功后，现会同步写入通用调度平台状态；此前只有 CPA 自动上传会点亮调度灯。

- 修复 Outlook 批量注册重复抢同一邮箱的问题：任务执行时优先复用任务记录里预绑定的 `email_service_id`，并新增 Outlook 账户 in-flight 占用保护，避免批量并发时多个任务同时选中同一个 Hotmail/Outlook 账户。

- 修复 LuckMail 批量并发复用同邮箱的问题：新增进程内 in-flight 邮箱占用保护，复用邮箱被当前批次任务拿走后会先占坑，任务成功/失败/删除时再释放，避免同一 Hotmail 被多个注册任务同时推进。

- 调整账号管理页“CPA”状态灯为通用调度平台指示：表头改为“调度”，CPA / Sub2API / Codex2API / Team Manager / NewAPI 任一上传成功后都会点亮蓝灯。
- 补齐账号页各调度平台上传后的持久化标记：单个/批量上传成功后统一写入平台导入状态，刷新列表后可直接看到已导入状态。

- 修复 Outlook / Hotmail 批量注册后台调度参数错位：补齐 Codex2API 与注册类型参数透传，避免启动批量任务即抛出 `run_outlook_batch_registration` 参数数量异常。
- 优化注册后自动上传日志：勾选 CPA / Codex2API / Sub2API / TM / NewAPI 时，上传过程除前端日志外也会同步写入后端终端日志，便于直接排查卡点。

- 新增 Microsoft 邮箱导入兼容：Outlook 批量导入现支持直接粘贴 Hotmail / Live 的 `邮箱----密码----client_id----refresh_token` 文本，导入时统一规范化邮箱大小写并复用原有注册链路。
- 修复 Microsoft 邮箱状态判定的大小写匹配：导入查重、Outlook 账户可注册判断与注册状态展示统一按邮箱小写比较，避免 Hotmail / Outlook 大小写差异导致重复导入或重复注册。

- 修复 OpenAI 注册 400 链路：主注册流程与 anyauto 流程按步骤补齐 `oai-device-id` 与完整 `openai-sentinel-token`，不再复用旧的空壳 sentinel 包装。
- 新增 Playwright 版 Sentinel 浏览器取 token 辅助，并补齐 `organization/select` 收尾逻辑与对应回归测试，降低 `create_account` / consent 收尾阶段失败率。
- 修复原生注册收尾分支的 `continue_url` 兜底优先级：优先复用 `create_account`/OTP 已返回的 consent 链路，避免误退回旧的 OAuth authorize 地址导致重定向链丢失。

- 修复 `start_webui.py`、`webui.py` 与设置加载过程中的 host / port / access password 环境变量兼容问题，提升本地启动稳定性。
- 修复本地已存在 PostgreSQL 实例时 start_webui.py 仍强行拉起 Docker 导致 5432 端口冲突的问题；当前会优先复用可直连数据库。
- 修复计划注册任务“立即执行”时的状态记录语义，避免仅触发任务就被误记为执行成功。
- 修复 Outlook 批量跳过已注册账号时的大小写匹配问题，避免大小写差异导致重复注册。
- 修复注册页自动恢复逻辑与 Outlook 状态刷新渲染问题，并同步清理相关异常提示文案。
- 修复通过设置页修改 Web UI 访问密码时未同步轮换 `webui_secret_key` 的问题。
- 优化账号页自动刷新轮询，页面隐藏时暂停、重新可见时恢复，减少重复请求与定时器残留。
- 调整批量注册统计口径：被取消的单任务按失败计入，便于直接查看成功数、失败数与成功率。
- 修复注册任务取消链路：单任务/批量任务取消时会同步写入 `task_manager` 与数据库状态，避免前端已终止但后端仍继续按正常失败流程收尾。
- 优化注册主流程的协作式取消检查，在创建邮箱、授权、提交流程、验证码校验、建号、收尾与自动上传等关键阶段更快响应取消请求。
- 重构并行批量注册调度方式：由“一次性提交全部子任务”改为“按并发额度持续派发”，取消批量任务后不再继续启动新的子任务，未启动任务会直接标记为 `cancelled`。

- 修复注册任务状态回收缺失：应用启动时会自动回收上次退出后残留的 `pending` / `running` 注册任务，分别改为 `cancelled` / `failed`，避免僵尸任务长期占用邮箱服务导致无法删除。

- 优化注册页最近任务展示：新增“最近注册任务”列表，并对启动时自动回收的历史僵尸任务打上“启动回收”标记，方便区分普通失败/取消与系统恢复记录。

- 补充仓库忽略规则：新增忽略 `.codex_tmp/`、`tests_runtime/` 与 `pytest-cache-files-*`，避免提交本地临时脚本与测试缓存。

- 补充仓库忽略规则：新增忽略 .omx/ 运行态目录，避免提交本地 HUD/通知/tmux 状态、指标与日志文件。
