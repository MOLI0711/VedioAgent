# MirrorCore 开发日志

这份日志记录实际实现、验证证据、遇到的问题和下一步。设计推演、单元测试、数据库集成和真实云端联调分别记载，不能互相替代。

## 2026-11-10：M1 基础与资源（完成）

### 实现

- 初始化Go模块`mirrorcore`，加入Gin、pgx、UUID与bcrypt依赖。
- 创建Gin API入口：request_id、结构化访问日志、统一JSON成功/错误格式、`GET /healthz`、`GET /readyz`。
- 配置校验HTTP地址、PostgreSQL URL、预置账户邮箱/密码和Cookie Secure开关。
- API使用pgx连接池；启动时以PostgreSQL advisory lock串行执行嵌入式迁移。
- 第一份迁移建立users、auth_sessions、videos、parse_tasks、upload_sessions、runtime_gates、outbox_events、inbox_events、kafka_rejections、conversations及questions等基础表，并启用pgvector扩展。
- 预置账户只在不存在时创建，密码使用bcrypt。会话与CSRF令牌为密码学随机值，数据库仅存SHA-256摘要；Cookie为HttpOnly、SameSite Strict，Secure可配置，绝对有效期暂设7天；注销校验CSRF并可重复执行。
- 资源API：用户视频列表/详情、解析任务查询、解析任务创建。解析任务请求限制在服务端profile；同一用户锁用于名额串行化，检查请求键后锁视频，分配task_version；任务、latest_task_id、配置快照及Outbox事件在同一事务提交。
- PostgreSQL持久化SQL位于`internal/repository/postgres`，application层负责profile验证、规范请求hash和配置快照；HTTP handler不直接访问业务SQL。
- 本地Compose启动PostgreSQL 17 + pgvector与Kafka 4.1.1单Broker。

### 验证

- `go test ./...`：通过，包含配置、随机token/CSRF摘要、request_id和cursor测试。
- PostgreSQL集成命令通过：

  ```sh
  TEST_DATABASE_URL='postgres://mirrorcore:mirrorcore-local-only@localhost:5432/mirrorcore?sslmode=disable' go test ./internal/application ./internal/httpapi -count=1
  ```

- 集成测试覆盖真实迁移、登录Cookie属性、CSRF拒绝、注销与撤销后401、跨用户视频统一404、同键并发唯一、同键不同请求返回409、task_version递增、每用户活动任务上限和Outbox一事务一条记录。临时用户、任务和事件在测试后清理。
- 本机Podman中的PG17及pgvector 0.8.7实际迁移成功；重复启动API后schema_migrations仍为单条。`GET /healthz`与`GET /readyz`分别返回200。
- Kafka 4.1.1容器启动并显示broker RUNNING；M2后续已通过background应用实际连接并投递/消费消息，详见下方M2日志。
- `go vet ./...`：通过；`go build -o /tmp/mirrorcore-api ./cmd/api`：通过。
- 集成结束后本地库保留预置demo用户；sessions、videos、parse_tasks、outbox均为0行。

### Review与待复核假设

- 技术设计没有指定登录会话期限，本轮暂用7天绝对期限；尚非用户逐项确认，应review。
- `voice-v1`、重建profile和6小时任务deadline是初始工程配置。当前profile只做本地受控校验，不表示流水线参数已完成。
- 预置用户只在首次启动时创建，修改`BOOTSTRAP_PASSWORD`不会覆盖已存在账户；后续需确定本地重置与部署密钥轮换方式。
- 当前迁移按普通SQL分号分割，只能处理现有DDL；加入函数、触发器或含分号字串时要换可靠迁移执行方式。
- 当前迁移只覆盖基础资源/认证/对话表，字幕、ASR账本、chunks、embedding、parse_result、Agent轨迹和content jobs仍需后续迁移。
- M1阶段记录时Kafka虽本地可用，但应用尚未实现Outbox relay、consumer、Inbox事务或位点提交；此项已由后续M2更新，不能把历史记录误读成当前状态。

### M1后续状态

原计划中的M2内容已开始实现，最新进展见下方记录。

## 2026-11-10：M2 消息交接与任务恢复（进行中）

### 实现

- 新增`cmd/background`，启动Outbox relay、Kafka Inbox consumer和解析任务维护循环；后台启动时自动确保本地主题存在。
- Outbox采用短事务`FOR UPDATE SKIP LOCKED`领取、`send_epoch`和owner围栏，Kafka发送在事务外；发送成功条件更新为SENT，失败退避1至60秒并加抖动。
- Inbox逐条严格验证消息Schema、任务/视频/版本身份；Inbox去重记录、任务WAIT_DISPATCH→READY及受理时间在同一DB事务提交，随后才手动提交Kafka位点。永久坏消息在同事务写`kafka_rejections`和DLQ Outbox。
- background内加入任务协调器：领取到期READY/WAITING/RETRY_WAIT任务时`run_epoch+1`并发放90秒租约；续租以数据库时间为准，只有当前owner、epoch、未过期租约可延期。提供受保护的WAITING、RETRY_WAIT、FAILED转移。
- 维护器每15秒将过期RUNNING转为RETRY_WAIT，超过任务总deadline的已受理工作置FAILED；WAIT_DISPATCH 24小时未受理置为DELIVERY_TIMEOUT。
- Inbox首次受理才开始6小时解析deadline，避免Kafka投递等待占用解析预算。
- Outbox relay增加同event_id未受理对账：已发送60秒后仍为WAIT_DISPATCH的任务重新入队；Inbox重复识别确保迟到重复消息不复活或重置任务。此轮新增SQL尚未实际跑数据库集成验证。

### 已有验证证据

- 前一轮M2代码完成后，曾在本地PG17/pgvector与Kafka 4.1.1上运行Outbox→Kafka→Inbox→READY→手动offset提交，并覆盖相同事件重放；Outbox集成检查覆盖发送失败退避与旧send_epoch迟到结果不能覆盖新owner。后台进程也实际把一条Outbox事件投递并使任务进入READY。
- 本轮更新后执行`gofmt`及`go build -o /tmp/mirrorcore-api ./cmd/api`、`go build -o /tmp/mirrorcore-background ./cmd/background`，均通过。
- 本轮未运行测试或新SQL集成检查；本地验收遗留数据已清理，数据库仅保留预置用户，auth_sessions/videos/parse_tasks/outbox/inbox/kafka_rejections均为0行。

### Review与当前边界

- 任务租约/重试预算状态转换已编码，但PARSE阶段真实handler尚未实现；background现阶段不调用`ClaimNext`，READY任务保持可查询，不能称作“已解析”。
- `ClaimNext`按数据库next_run_at/admitted_at排序并使用SKIP LOCKED；当前没有并发工作槽、heartbeat协调循环与阶段提交器，阶段调用接入时必须把失租取消及时传给子进程/HTTP请求。
- 当前schema的`deadline_at`在消息首次受理时重设为6小时；未受理任务的24小时期限用`created_at`判断。日后如需独立调整，应显式新增迁移字段，不复用task_version或run_epoch。
- 新增的租约与未受理重投SQL应补数据库集成验证：A持epoch1过期，B领取epoch2，A Renew/Retry/Fail均拒绝；无Inbox事件时同event_id重投；超过24小时任务/Outbox终止；Kafka重复投递后READY保持原阶段。
- 外部OSS、Fun-ASR、Embedding和Qwen仍未配置凭据，没有云端或费用验证。

### 下一步

1. 对任务租约、过期回收、deadline、未受理补投，以及上传名额和取消终态增加PostgreSQL集成验证。
2. 配置阿里云OSS私有Bucket、RAM最小权限、CORS和未完成multipart生命周期，联调真实签名头、分片上传、完成后核验及取消。
3. 设计并实现PROBE/AUDIO阶段产物表、租约下不可变产物复用和FFmpeg边界，再接Fun-ASR持久提交/查询/结果保存。
4. 只有完整阶段结果能被恢复与校验后，才接入字幕、chunk、Embedding和单调发布。

## 2026-11-10：M3 视频上传首版（代码已写，待数据库/云端验证）

### 实现

- 增加OSS Go SDK V2适配器。私有Bucket multipart由API初始化；每次只为有限分片编号签发短期UploadPart URL，固定签名Content-Length并返回签名头；Go本身不代理视频字节。
- 上传会话按创建键幂等；complete请求携带客户端从每次成功UploadPart响应记录的ETag。进度接口不公开ListParts的ETag，响应丢失时需重传该片。服务端读取OSS ListParts并校验分片编号、数量、每片大小和ETag一致性，再用已核验的客户端ETag调用CompleteMultipartUpload。完成响应不明时先HEAD唯一对象，核对大小和`upload-session-id`/`video-id`元数据。
- 上传确认后在一个PostgreSQL事务登记源blob、建立task_version 1、更新视频source/latest字段、将上传预留转换为活动解析任务，并写入Outbox。重复complete返回同一initial_task_id。
- 增加`DELETE /v1/uploads/{id}`。取消先写数据库意图并停止签发新URL；后台等30分钟（15分钟URL有效期加15分钟请求收敛窗口）及操作租约过去后，递增operation_epoch并调用AbortMultipart。OSS成功后会话进入ABORTED/CANCELLED或EXPIRED，释放配额；未知upload_id的未完成分片依赖Bucket生命周期清理。
- 新增`000002_upload_parameters.sql`和`000003_upload_termination.sql`。OSS空配置时上传接口禁用；`env`凭据来自`OSS_ACCESS_KEY_ID`、`OSS_ACCESS_KEY_SECRET`与可选`OSS_SESSION_TOKEN`，ECS可使用RAM角色凭据提供者。

### 验证证据

- 本轮执行`gofmt`、`go mod tidy`、`go build -o /tmp/mirrorcore-api ./cmd/api`和`go build -o /tmp/mirrorcore-background ./cmd/background`，均通过。
- 本轮未运行测试或本地数据库迁移；新增上传SQL、锁顺序、配额转换和终止维护未作PostgreSQL故障场景验证。
- 没有阿里云凭据，未进行真实OSS签名、浏览器CORS、分片/complete/abort、RAM权限或费用联调。不能将SDK代码编译通过表述为OSS已接通。

### Review与边界

- OSS与PostgreSQL不能共用一个事务；实现用唯一对象key和会话元数据做HEAD对账，DB提交时再原子创建blob、首个任务和Outbox。
- 签名URL是有时限的持有者能力，取消后不再签发；回收延迟30分钟，按15分钟URL有效期加15分钟请求收敛窗口估算。后15分钟无法证明覆盖所有网络中的上传，超时后仍需依靠Abort和生命周期清理；部署前应以客户端与OSS实际请求上限复核。
- INITIATING响应丢失且DB没有upload_id时无法定向Abort。新会话状态会在宽限后终止并释放名额，但远端孤儿MultipartUpload必须由OSS生命周期规则清理；建议至少设置3天未完成分片清理并在真实Bucket确认规则生效。
- `UploadMaxDuration`目前只是后续PROBE的业务上限常量，上传声明无法证明真实时长。上传完成后视频是UPLOADED，尚未成为VALID；必须等PROBE检查格式、编码、时长、时间轴后才能启动后续处理。
- 成功complete会返回WAIT_DISPATCH任务。M4接入PROBE/AUDIO handler；ASR及后续阶段仍未接入。

## 2026-11-10：M4 PROBE/AUDIO媒体Worker（代码已写，待数据库/媒体/云端验证）

### 实现

- 新增`000004_media_pipeline.sql`：`videos.probe_metadata`、blob的SHA/产物指纹/profile hash/元数据、`task_steps`阶段检查点及READY复用索引。每个同视频同产物/profile至多登记一个READY产物。
- 将媒体阶段操作从上传仓储拆出`StageRepository`。阶段开始、失败、PROBE提交、AUDIO提交及损坏产物标记都按用户→视频→任务锁顺序，并检查owner、task stage、run_epoch、lease_owner、租约和总deadline；外部OSS与FFmpeg不持数据库事务。
- background启动两个数据库轮询Worker槽位，只领取PROBE/AUDIO；90秒租约每20秒续租。续租失败取消当前下载/FFmpeg context；停机时尽量把任务释放到RETRY_WAIT，无法确认时由租约维护恢复。PROBE完成后任务原子推进到AUDIO；AUDIO完成后推进到ASR，Worker不会反复领取尚未支持的ASR阶段。
- Worker启动要求同时配置OSS且PATH中可找到`ffmpeg`、`ffprobe`。启动时记录工具版本并纳入profile hash，工具升级会使旧媒体产物不再被错误复用。OSS读取可固定object version；所有本地文件在task_id/run_epoch私有临时目录内。源文件上限2GiB、音频上限231MiB；单任务预留3GiB，开始领取前要求保留10GiB可用磁盘空间；全局FFmpeg信号量为1。
- PROBE先校验私有OSS对象的大小、版本和video-id元数据，再有界下载并流式计算SHA-256，调用ffprobe检查MP4容器、H.264视频、时长不超过120分钟、AAC音轨及各轨道相对视频的`audio_origin_ms`。默认AAC候选优先；AUDIO逐条尝试解码并验证WAV规格，失败后继续下一候选。无音轨记录NO_AUDIO并推进ASR。
- AUDIO以源SHA、选中的stream index、codec和音轨时间偏移计算产物指纹。先查询数据库中READY音频并HEAD核对对象版本、大小和上传时记录的SHA/profile/video元数据；有效则直接登记本task的检查点。没有可复用对象时，FFmpeg生成16kHz、单声道、pcm_s16le WAV，经ffprobe核验编码、声道、采样率、时长与大小；本地计算SHA-256后使用不可变唯一OSS key上传，成功后HEAD核验，并在租约围栏内把blob与阶段检查点一并提交。OSS上传SDK默认启用CRC64传输校验。
- stage失败按阶段计数并退避1、2、4、8秒；不可重试错误或累计5次后任务FAILED。明确媒体/源对象损坏时视频改为REJECTED；瞬时OSS错误不会误标成格式错误。

### 验证证据

- 本轮执行`gofmt`与`go build -o /tmp/mirrorcore-api ./cmd/api`、`go build -o /tmp/mirrorcore-background ./cmd/background`，构建通过。
- 本轮没有运行测试或数据库迁移，`000004_media_pipeline.sql`、新增任务租约/上传SQL和事务锁顺序尚未由PostgreSQL验证。
- 当前开发机未安装`ffmpeg`/`ffprobe`，无阿里云OSS凭据；所以Worker在此环境明确停用，未处理真实媒体，未发生云调用或计费。构建通过只证明代码可编译。

### Review与边界

- PROBE/AUDIO外部对象写成功而DB提交失败时，对象仍是未登记的孤儿候选，不能被解析任务当作完成产物。新执行按DB READY引用和指纹检查；没有登记的对象需由后续GC按对象引用图回收。OSS与PostgreSQL没有跨服务事务。
- 音频产物SHA在本地计算，SDK CRC64用于上传传输校验，HEAD只验证对象存在、版本、大小及自定义元数据，不重新下载整份对象计算SHA。对象使用唯一key及禁止覆盖；ASR接入时仍须对下载音频流重新计算SHA后再提交供应商。
- PROBE检查视频流类型和时长，FFmpeg当前完整解码被选中的AAC音轨；并未逐帧完整解码H.264视频。这是首版语音解析的验证边界，未来如需验证视频内容完整性需单独增加解码成本预算。
- `task_steps`和READY blob具有持久化checkpoint，task_version/run_epoch分别表示业务配置版本和某次执行资格；被后续任务复用的音频通过source SHA、音轨选择、工具版本/profile hash区分，不要求等待“最新”task_version完成。
- 仍未实现Fun-ASR提交/状态查询/结果保存、转写归一化、字幕、chunk、Embedding、结果单调发布、问答Agent、SSE、章节摘要、删除GC和生产监控。

## 2026-11-10：M5 Fun-ASR提交、恢复与原始结果持久化（代码已写，待验证）

### 实现

- 新增`000005_asr_pipeline.sql`与`asr_submissions`账本：固定音频/配置指纹、submit_seq、预留operation_id、远端task/request ID、首次两小时deadline、unknown时间、下次轮询、结果下载预算和raw blob引用。每个task最多两条提交记录；`RESULT_READY`表示远端已结束但本地结果尚未持久化。
- 新增Fun-ASR北京HTTP适配器。POST使用`fun-asr`、单个`file_urls`、mono `channel_id:[0]`、Bearer API Key和异步header；禁用POST重定向，代码没有POST自动重试。传输超时、无task_id的成功响应及5xx按提交结果不明处理，明确4xx拒绝记录在账本中；429保存并尊重Retry-After，最多使用第二次提交预算。
- 在POST前先生成6小时版本固定的OSS GET签名URL，并核对签名期覆盖至少2.5小时、临时凭证额外留有30分钟余量；签名窗口不足时不发API请求。签名音频URL仅位于请求内存，不保存到日志和数据库。
- Worker在同一个两槽任务池中接入ASR阶段。provider gate行先锁，再锁owner/video/task；UNKNOWN也占provider在途名额。worker接管先恢复提交账本：已知task_id只查该ID；不明SUBMITTING等待租约窗口后变UNKNOWN，等5分钟才可消耗唯一补交机会。长轮询先写WAITING/next_run_at并清租约，释放本地槽位。
- 正常远端状态查询超时10秒，轮询5/10/20/30秒并加稳定抖动；远端成功后先记RESULT_READY并释放provider in-flight slot，再校验单文件子任务并下载不超过64MiB的transcription JSON。下载最多3次，403/404转ASR_RESULT_EXPIRED。
- 原始JSON上传OSS唯一任务/提交对象key，写SHA、输入指纹、profile和submission ID元数据；事务提交失败后按稳定key恢复，HEAD验证后下载重算SHA并再次验证JSON。PostgreSQL受run_epoch围栏地登记ASR_RAW blob、完成提交和ASR task_step，再推进到TRANSCRIPT。无音轨时写NO_AUDIO并跳过供应商。
- `.env.example`加入百炼配置；后台只有OSS和百炼配置同时存在时启用ASR。缺少百炼配置时ASR任务仍保留在TRANSCRIPT前的ASR阶段，不做模拟结果。

### 验证证据

- 本轮执行`gofmt`与`go build ./...`，构建通过。
- 未运行测试、数据库迁移或SQL集成验证；未调用真实OSS或百炼。M5 migration仅加入文件，未应用到开发数据库。
- 本机缺少`ffmpeg`/`ffprobe`及OSS/百炼凭据，因此没有音频上传、签名URL访问、provider调用、账单或ASR结果恢复运行证据。

### Review与边界

- PostgreSQL事务无法与Fun-ASR POST原子化。预留后进程退出会保守消耗一次提交预算；网络错误变UNKNOWN；这仍不能避免远端已经受理但本地不知道的重复费用，只把自动补交限制在最多一次。
- RESULT_READY只表示provider任务确定完成。raw结果只有在下载、校验、上传OSS和DB引用全部完成后才使ASR task_step READY。provider并发gate中RESULT_READY不再计入在途；未知的远端状态仍保留名额。
- OSS成功和数据库失败之间仍有孤儿对象窗口；固定对象key和OSS元数据允许同一run恢复，其他未登记对象未来需由引用图GC回收。
- 本地时间用于Worker等待决策，关键资格及provider gate操作仍由事务与数据库租约围栏约束；应在数据库集成验证中覆盖进程重启、UNKNOWN满额、第二次提交与结果持久化中断。
- ASR实际模型调用未开通；当前profile固定provider/model/region/channel，但Fun-ASR型号本身由服务端维护，未来供应商变更需更新profile版本和配置。
- 官方协议参考：[百炼Fun-ASR录音文件识别HTTP API](https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-http-api)。

## 面试问题记录：M4

### 为什么Kafka consumer不直接处理视频，而由数据库扫描Worker领取任务？

Inbox consumer只做身份校验、去重和任务准入后提交offset。媒体下载和FFmpeg是长流程，交给数据库任务状态、租约和阶段检查点管理，避免Kafka重平衡/位点和长任务生命周期耦合。Kafka负责可靠传递请求，PostgreSQL任务行负责执行调度与恢复。

### `task_version`和`run_epoch`分别解决什么问题？

`task_version`是用户触发的业务处理版本；`run_epoch`是某个版本第几次获得执行权。临时失败重试保持task_version不变，重新领取递增run_epoch。阶段提交在短事务中校验当前epoch、owner和未过期租约，旧Worker迟到不能覆盖新Worker。

### FFmpeg完成了，数据库提交却失败，系统怎样恢复？

音频先上传到OSS不可变唯一key；只有DB围栏事务成功登记blob并标记task_step READY后，任务才会使用它。事务失败的对象没有数据库READY引用，不能视作完成；重试先找已登记可复用对象，否则重新生成新key，孤儿对象由未来GC清理。

### 重建任务怎样复用上次音频，又怎样处理默认音轨损坏？

音频指纹包含源视频SHA、选中的音轨索引、codec、时间偏移和工具/处理profile。新task先查READY blob并核对OSS版本、大小和metadata；复用后写自己的task_step。PROBE按默认标记排序AAC候选，AUDIO实际解码并检查PCM WAV，首选失败后尝试下一轨；选中轨道的`audio_origin_ms`保留给转写时间映射。

### 当前实现能证明音频对象内容与SHA完全一致吗？

上传前会流式计算SHA-256，OSS SDK默认执行CRC64上传传输校验，HEAD再检查对象版本、大小和我们写入的SHA/profile元数据；HEAD不会重新读取对象并重算SHA。对象key唯一且不可覆盖，ASR阶段仍应下载时重算SHA并在不匹配时禁止提交。

### M4哪些内容尚未实际验证？

新迁移和围栏事务未由数据库执行验证；本机缺少FFmpeg与OSS凭据，没有真实媒体处理、上传或计费证据。当前仅有`gofmt`及两个Go二进制构建通过的证据。

## 面试问题记录：M3

### 为什么浏览器分片直传OSS，而不是把视频上传到Go再转发？

避免Go API承担2GiB请求的网络带宽、内存/临时磁盘占用和长连接生命周期。Go仍控制对象名、会话、每片授权、配额与完成校验；浏览器获得的是固定对象、upload_id、part_number和短有效期的UploadPart URL，不是通用Bucket写权限。代价是要处理CORS、短期URL泄露及OSS/DB两个系统之间的一致性。

### 为什么complete不能相信浏览器报告的分片完成情况？

浏览器只用于上传和展示进度，可能断线、重放或报告错误。客户端从成功UploadPart响应记录每片ETag；服务端按数据库中的object key/upload_id从OSS ListParts核实数量、编号、大小和ETag是否一致，再用已核实的客户端ETag组装Complete请求。进度接口不回传ListParts的ETag；分片ETag也不作为源视频SHA-256。

### OSS已经完成对象、数据库却提交失败时如何恢复？

外部OSS完成和PostgreSQL事务不存在共同原子提交。接口超时或DB失败时会保留可恢复状态；重试先HEAD固定object key，验证完整大小及会话元数据。对象有效则继续在DB事务中创建blob、首任务和Outbox；若事务其实已提交，initial_task_id使重放返回原任务。

### 用户取消上传后，为什么不立即AbortMultipart？

取消先把会话变为不可签发状态，但先前发出的预签名URL仍可能在过期前使用，且HTTP上传可能仍在途。后台等待签名宽限和操作租约后再执行Abort；若进程在Abort后、写终态前退出，租约到期后可重试。未知upload_id的孤儿分片无法定向取消，需由OSS生命周期策略兜底。

## 面试问题记录：M2

### 为什么Kafka offset要等数据库事务提交之后再提交？

数据库是任务状态与阶段进度的权威来源。如果先提交Kafka位点，进程可能在数据库提交前退出，消息就不再重放且任务没有可靠记录。先提交Inbox和READY，再提交offset；若offset提交失败，Kafka重放由Inbox唯一键识别。两种顺序都可能重复，但不能形成“位点已前进、数据库无记录”的丢失窗口。

### 为什么Kafka consumer不直接跑完整ASR？

消息消费和媒体处理生命周期不同。consumer只做短事务的身份校验、Inbox持久化和任务准入，然后提交位点；Worker用数据库任务状态、租约、epoch和检查点执行长流程。Kafka重平衡或消费进程重启就不会中断一个被Kafka消息生命周期绑住的长任务，也不用把位点提交推迟几小时。

### run_epoch怎样阻止旧Worker覆盖新执行？

每次获得数据库执行权时递增epoch。A以epoch 1运行期间租约过期，B领取时得到epoch 2。所有业务写入都要在短事务中锁任务并验证RUNNING、owner、epoch和未过期租约；A迟到后条件不成立，不能写入。该机制保护数据库提交，不会撤销A已经发出的外部ASR请求。

### 为什么租约过期后不能让原Worker续租？

租约到期代表执行权可以被别人接管。若A能在超时后恢复网络并把旧租约续活，A和B就都可能认为自己有权继续。续租必须要求原lease_until仍在未来；一旦失效，旧执行停止并由新领取递增epoch。

## 2026-11-10：M6 字幕归一化、连续位点和产物发布（代码已写，待数据库/云端验证）

### 实现

- 新增`000006_transcript_pipeline.sql`：创建带输入/配置指纹的transcripts产物、transcript_segments句段表和`task_steps.output_transcript_id`引用；状态区分BUILDING、READY、NO_SPEECH、BROKEN。数据库约束限制时间区间、序号、文本和最多100万句。
- 新增TRANSCRIPT阶段Worker。它读取ASR提交的raw blob与音频产物引用，先HEAD并版本固定读取私有OSS对象，核对输入指纹、profile、大小、对象版本和SHA-256，再运行现有Fun-ASR JSON结构校验。
- 固定`video-transcript-normalizer-v1`配置：裁剪首尾空白、忽略空白句、按局部起始时间稳定排序，使用可正可负的`audio_origin_ms`转换到原视频时间轴。阿里云文档定义`begin_time`/`end_time`单位为毫秒；句子必须满足`0 <= start < end`且落在源视频长度范围。部分句子起止边界越界最多1000ms时裁剪并记录修正量，完全位于视频时间轴外的句子不生成字幕行但记录数量，其他部分越界或零长度转INVALID_TRANSCRIPT。
- 句段通过provider原序索引与归一化`seq`分别追溯输入及展示顺序。首次实现批次上限为200条；`transcript_segments`写入和`last_committed_seq`推进在同一短事务完成，并在相同事务内锁任务、校验owner/run_epoch/租约。恢复时重新读取固定来源并比较已提交连续前缀，确认无缺行/乱序/内容变化后从位点后续写。
- 全部句段与位点完整后，在租约围栏事务中校验行数、连续位置和状态，发布transcript产物及TRANSCRIPT task_step，并推进到CHUNK。DB事务失败不会把部分BUILDING句段当作完整字幕使用。没有音轨时生成NO_SPEECH字幕终态并在metadata标记NO_AUDIO_STREAM；有音轨但无有效文本时标记NO_RECOGNIZABLE_SPEECH。
- background在OSS配置存在时注册TRANSCRIPT处理器，允许已有持久化ASR检查点在无模型凭据时继续恢复；新增阶段共用现有两槽Worker和租约续期逻辑。
- 按用户要求，把M6实现原则、故障恢复边界和面试问答同步记录到`MirrorCore项目设计与问题记录.md`及本交接文件；README描述当前可恢复阶段与运行限制。

### 验证证据

- 本轮执行`gofmt`和`go build ./...`，构建通过。
- 未运行测试、数据库迁移、SQL集成或云端调用；`000006_transcript_pipeline.sql`未应用到开发数据库。本机没有FFmpeg或OSS/百炼凭据。
- 因此当前只有代码可编译证据。阿里云文档已确认时间戳单位为毫秒；数据库事务约束、真实Fun-ASR响应边界、OSS版本固定读、整条媒体/ASR/字幕恢复链路均未运行验证。

### Review与边界

- 首版选200条作为事务批次上限，目的是限制单事务写入规模；没有基准测试依据，后续应以实际句数分布、事务耗时和锁等待调整，并保留指纹版本变更。
- 有效ASR原始JSON是归一化恢复源；完成句段可跨同一视频不同task_version复用，不依赖旧Worker本地缓存。`run_epoch`仅决定当前谁能修改DB，不参与内容身份。
- 时间映射假设AUDIO阶段未剪除中间静音或改变速度；`audio_origin_ms`可为负，表示音轨先于视频起点。完全落在视频时间轴外的句子仅从规范化字幕中略过，仍可从原始JSON追溯。若未来做变速/分段，需要持久化完整时间映射，不能继续只加一个零点偏移。
- 数据库异常或意外人工改动若导致BUILDING前缀与源内容不符，本轮把产物标BROKEN并在后续重试时清空重建；已发布产物若发现不一致会失败关闭，不自动覆盖可能已被下游引用的数据。
- transcript代码只把任务推进到CHUNK。切分、Embedding、结果清单、问答和Agent均未实现；不报告识别准确率或处理性能。

## 2026-11-10：M7 CHUNK切分、来源和可恢复集合（代码已写，待数据库验证）

### 实现

- 新增`000007_chunks.sql`，建立`chunk_sets`、`chunks`与`chunk_sources`，为`task_steps`增加`output_chunk_set_id`。组合外键确保chunk_set、video、transcript与源句段一致；chunk文本保存SHA-256、字节长度和原视频时间；源范围是transcript文本中的半开UTF-8字节区间。
- 后续核对选型发现：百炼托管text-embedding-v4没有提供可证明与本地分词器等价的版本。遵照设计文档已写的降级路线，采用`chunk_v1_bytes`，按UTF-8字节计量；不把本地Qwen tokenizer计数宣称为v4 tokens，也不把字节值视作token限制的数学上界。目标512、硬上限768、最多重叠1条完整原句且不超过128字节；参数是初始profile，不是效果结论。
- 切分按字幕顺序合并，计量最终拼接文本（包括换行分隔符）；超长原句优先从标点拆分，再按UTF-8安全边界递归细分。每个chunk保留transcript seq与原句字节范围；句级时间不足以细化时继承原句时间。被拆开的超长句片段不作为跨chunk重叠单位；每个新chunk都必须消耗新的来源文本。
- 新增CHUNK处理器并接入既有两槽Worker。输入固定到READY/NO_SPEECH transcript；chunk输入身份包括transcript及其指纹，配置指纹覆盖算法版本、字节参数、度量单位与计数器版本。
- `BUILDING`集合按当前逻辑task隔离，重试时重算切分计划、验证已提交连续前缀及来源范围，只补缺失行。READY/NO_CONTENT集合逐片验证后可由同视频、同字幕和同配置的其他task复用。chunk ID由set ID与seq确定性生成；批次最多100片，批次事务和最终发布都检查当前run_epoch、owner和有效租约。
- 最终事务验证数量、seq、正文摘要、长度、时间与来源映射，将集合置为READY或NO_CONTENT，写CHUNK task_step并原子推进EMBEDDING。空字幕不调用Embedding；Embedding处理器尚未实现。
- 修正了首次实现中误将其他Qwen tokenizer视为v4计数器的实现偏差：移除本地tokenizer资产及依赖，改用文档已有的字节profile，避免面试口述将byte count说成token count。

### 验证证据与缺口

- 本轮运行`gofmt`、`go mod tidy`、`go build ./...`，均通过；没有运行测试、数据库迁移或外部API调用。
- `000007_chunks.sql`未在本轮应用数据库。没有验证PostgreSQL迁移、跨行外键、部分唯一索引、run_epoch批次恢复或跨task复用；Embedding API输入限制和字节profile效果也未实测。
- CHUNK完成后任务进入EMBEDDING；由于该处理器尚未接入，当前纵向链路在这一阶段等待。接下来实现embedding-v4请求契约、固定批次身份、操作预算和已提交向量核验，再进行完整集合和单调结果发布。

## 2026-11-10：M8 Embedding API与pgvector批次恢复（代码已写，待数据库/云端验证）

### 实现

- 新增`000008_embeddings.sql`：创建`embedding_sets`、`chunk_embeddings(vector(1024))`和规范化的`embedding_batches`预算表；扩展`task_steps.output_embedding_set_id`并要求产物阶段匹配。批次主键为`task_id + chunk_set_id + config_hash + batch_index`，批次区间和输入fingerprint不可变，attempts数据库约束在0至3。
- 新增百炼北京text-embedding-v4同步HTTP客户端，按官方DashScope原生字段发送`input.texts`与`parameters.dimension=1024/text_type=document/output_type=dense`，解析`output.embeddings[].index`、向量、request_id和usage。Transport复用连接，单请求30秒，禁止重定向；429解析Retry-After并限制为15分钟内。
- 新增EMBEDDING stage处理器并接入同一两槽Worker。配置hash绑定provider、地域、workspace、模型、角色、输出类型、维度、固定批大小和最大尝试次数；不记录API key。
- 固定chunk seq每10条为一个批次。调用前通过租约围栏事务持久预留批次attempt；每批最多3次，Worker接管不会重置。Embedding同步HTTP在事务外；校验response index完整且无重复/越界、每向量1024维、有限且非零之后，才在同一短事务写pgvector明细和SUCCEEDED预算账本。同一输入配置下首个有效持久向量作为规范产物，不要求重试结果逐位相同。
- 同一任务重启时核对已成功批次向量及输入hash后跳过；还未持久提交的批次按原身份继续。发现持久行失配会清理该批无效向量，并且只有原预算尚未耗尽时才重算。向量文本传入`::vector(1024)`，读取`embedding::text`校验输入hash、维度及数值有效性，没有新增pgvector-go依赖。
- READY/NO_CONTENT embedding_set通过完整性检查后写EMBEDDING task_step并推进PUBLISH。NO_CONTENT不发空请求；未配置百炼凭据时空集合仍可完成，非空集合会以`EMBEDDING_PROVIDER_NOT_CONFIGURED`明确失败。
- Worker阶段失败接收`Retry-After`，和本地stage退避取较大者并受15分钟上限控制。Embedding批次API尝试预算与现有阶段逻辑失败上限分开累计。
- 更新README、主设计问题记录及H8交接文档；追加批次账本、恢复、同库原子写入和外部计费边界的面试问答。主设计文件继续同步到桌面。

### 验证证据

- 本轮执行`gofmt`和`go build ./...`，编译通过；没有运行测试。
- 未运行`000008_embeddings.sql`；本轮M3至M8迁移均未应用数据库。没有验证pgvector参数编码、组合外键、唯一索引或事务故障恢复。
- 没有百炼API Key/Workspace凭据，没有发起请求或产生计费；接口字段依据官方[同步Embedding API文档](https://help.aliyun.com/zh/model-studio/text-embedding-synchronous-api)实现，但尚未用本项目账户核对真实HTTP响应。
- 没有FFmpeg/ffprobe与OSS凭据，媒体全链路未运行。构建只证明代码编译，不证明迁移、SQL、真实provider或恢复行为有效。

### Review与边界

- 固定批次保存整批向量与批次账本于同一个PostgreSQL事务，所以本实现的正常断点是完整批次；预算身份不按本次缺失子集重算。若数据库中的成功批次存在部分/损坏行，清理后按固定批次重算，仍占原预算。
- 供应商同步调用与PostgreSQL无法共同事务。HTTP成功后、向量事务提交前进程退出仍可能重复计算/计费；三次持久预算限制风险，不提供端到端恰好一次。
- NO_CONTENT在缺百炼凭据时能继续到PUBLISH；非空文本任务则快速、明确终止，部署前需配置同Workspace/API Key。
- 当前一次性加载本任务所有chunk以核对完整集合，表容量上限为100万条。最大规模可能占用较多内存；进入真实环境前应收紧产品规模上限或优化为流式读取。
- `000008`未应用，主设计/桌面文档已更新。结果清单及单调版本发布尚未实现；下一阶段实现PUBLISH并校验TEXT与NO_SPEECH两类完整结果。

## 2026-11-10：M9 结果清单与 task_version 单调发布（代码已写，待数据库验证）

### 实现

- 新增`000009_publish.sql`：建立`parse_results`结果清单表，记录task/video/version、TEXT或NO_SPEECH、字幕/片段/向量集合ID、各自数量及配置快照；同一task与同一视频版本各自唯一。通过`(task_id, video_id, task_version)`复合外键确保版本确属该任务。应用层只创建或核对既有清单，不更新既有清单。
- 增加`videos.active_result_id`、`task_steps.output_result_id`和`questions(result_id, video_id)`复合外键，确保生效指针、PUBLISH检查点和问答版本不能跨视频引用。扩展task_steps产物stage检查，只有PUBLISH步骤能保存结果ID。
- 新增PUBLISH处理器并接入两槽background Worker。它不调用外部模型；同一事务中锁定用户、ACTIVE视频及RUNNING解析任务，验证lease/run_epoch，读取已完成的TRANSCRIPT、CHUNK、EMBEDDING检查点及关联产物。
- 发布前比较检查点与产物状态、输入fingerprint、config hash、集合间引用、字幕/片段/向量实际数量，并验证每个chunk都有对应字幕来源。只接受完整TEXT结果或合法空的NO_SPEECH结果；不完整产物永久失败关闭。
- 每个任务至多创建一份清单；重入时核验既有清单与当前产物完全一致。锁定video后读取active结果版本，仅`task_version > active_task_version`时更换指针。无旧指针时首个版本可以上线。较旧版本仍保存清单并正常SUCCEEDED，记录`SKIPPED_OLDER`而不覆盖较新版本。
- 清单、active指针、PUBLISH task_step结果和任务SUCCEEDED在同一PostgreSQL事务中提交。PUBLISH无供应商调用；事务或租约围栏失败不会部分发布。
- 更新README、H9交接任务书与主设计问题记录，补充并行版本例子、面试表达与验证缺口。主设计文件继续同步到桌面。

### 验证证据与缺口

- 执行`gofmt`与`go build ./...`，均通过；没有运行测试。
- 没有应用`000009_publish.sql`，M3至M9新增迁移均未在本轮应用数据库。没有运行SQL约束、复合FK或真实事务验证。review发现迁移执行器以分号拆分SQL，因此没有在当前迁移中加入需要PL/pgSQL函数体的不可变触发器；数据库层不可变保护仍是后续可加固项。
- 没有执行两个解析任务并发发布、较旧版本迟到、commit响应不明、源/来源缺失、租约失效等数据库故障场景；没有连接OSS、百炼或实际媒体环境。
- 编译通过只能证明Go类型和依赖可构建，不能证明SQL与PostgreSQL实际兼容或并发语义已经通过实验。

### Review与下一步

- 用户确认的发布规则是“完成且版本大于当前之前版本就上线”，不是严格等待最新已创建版本。实现读取当前active指针并比较版本；视频行锁使并行更新串行，不用比较可能仍在排队中的`latest_task_id`。
- `task_version`表达业务产物版本；`run_epoch`继续只表达当前执行资格。结果清单记录一次成功解析，视频指针决定当前在线版本，问题记录的复合外键固定问答使用的清单。
- 下一阶段实现会话与固定结果问答：创建请求时固定`result_id`，检索限制同一embedding_set，证据登记为可引用ID，答案在结构和引用校验后持久化；之后开发有限Agent/SSE及取消与重连。

## 2026-11-10：M10 固定结果问答与会话（代码已写，待数据库/云端验证）

### 实现

- 新增`000010_questions.sql`：为已有会话表增加长度/指纹约束；为问答与结果、结果与转写补足复合唯一关系；创建`question_steps`与`question_evidence`，约束问答证据必须属于固定结果及转写。
- 问答受理按`Idempotency-Key`幂等，锁用户/视频/会话，受理时固定active `result_id`及对应transcript、embedding集合、generation/retrieval profile与会话上下文快照；同一用户同时只有一个RUNNING问题。
- 新增阿里云百炼`text-embedding-v4` query角色调用，PostgreSQL精确cosine Top-K=8检索限定于固定embedding_set。把命中chunk映射回原转写句，前后各补齐最多1句，保存E编号、正文hash和时间范围。
- 新增Qwen北京Chat Completions固定快照及strict `answer_v1` JSON Schema。结构化响应拒绝重复JSON键、额外字段、非法状态、越界长度、未知引用；引用映射从Go已登记证据生成，不接受模型自带来源。
- 新增conversation创建幂等、列表/查找、问答历史、归档与问答取消接口；问答SSE单写者发送accepted、progress、完整答案、done/error，答案只在校验且数据库提交后发送。断线/取消和租约维护收敛终态，重放成功请求回放已保存答案。
- Worker/API启动问答依赖已配置时启用；缺少模型密钥时不会伪造本地答案，非空文本问答明确返回provider未配置。

### 验证证据与缺口

- M10代码当时执行`gofmt`与`go build ./...`通过；没有运行测试、`000010`迁移、PostgreSQL故障场景或百炼调用。M3至M10迁移均未在本轮应用数据库。
- 没有OSS/百炼凭据和媒体工具，所以固定结果问答没有真实数据、供应商返回、费用、召回效果或SSE取消实测。
- 受理事务、幂等竞争、断线与提交竞争、过期租约、JSON Schema实际能力及Embedding `text_type=query`仍需在测试和真实环境验证。此记录明确区分代码实现、构建通过与运行验证。

## 2026-11-10：M11 固定版本上的受控Agent工具循环（代码已写，待协议/数据库/云端验证）

### 实现

- 扩展Qwen Chat Completions provider，支持带工具的决策请求、strict JSON Schema收尾和保留完整assistant `tool_calls`及后续tool消息的多轮上下文。请求设置`parallel_tool_calls=false`，应用仍检查多个调用，不依赖模型遵循该开关。
- Go仅执行`search_video`与`read_context`。参数严格解码、拒绝重复JSON键和额外字段；检索工具只通过固定视频、transcript与embedding_set执行，read_context的证据ID必须在本次question evidence registry中。
- 新证据先按来源句去重，再在固定transcript范围内登记，递增E编号；累计上限24句/24KiB。补查不产生新证据、重复动作、预算或时间临近时停止，最终依据已有材料收尾。
- 逻辑模型调用最多3次，工具最多2次，Qwen HTTP请求最多4次且全问答最多一次短重试；初检及两次search合计最多6次query Embedding HTTP尝试。检索与工具阶段为最终调用预留45秒。每个模型、工具、校验动作都写question_steps输入hash、结果码、provider request ID、usage和受限metadata；不持久化模型私有分析文本。
- 最终答案验证失败时，仅在剩余逻辑调用预算内做一次schema/引用修复。答案依然通过Go来源映射后才和SUCCEEDED一同保存，再向SSE发送。
- 增加`ReadEvidenceContext`参数化查询，以及QuestionStep metadata的事务性持久化；没有新增数据库迁移。

### Review发现与处理

- Function Calling必须保存完整assistant tool_calls，并对每个ID追加匹配的tool响应；如果模型一轮返回多个动作，不部分执行，记录协议错误、为各个有效ID回填错误观察后转入最终收尾。
- search_video候选可能映射到多个字幕句，初检及每次向量检索将主来源限制为最多8句，再做有限邻句扩展，避免超过`ExpandEvidence`数据库输入上限；read_context不再次自动扩窗，严格返回模型请求的窗口。
- 时间预算需按调用实际最长时间推导：工具搜索最多两次15秒Embedding尝试加数据库读取及保存，因此为最终45秒之外预留40秒；read_context预留4秒。每轮决策API timeout按固定question deadline截短，保持收尾余量。
- 问答记录的模型决策轮、HTTP重试、工具动作和query Embedding分别计数，避免同一“次数”混淆成本与业务预算。

### 验证证据与缺口

- 本轮执行`gofmt`与`go build ./...`通过；按当前工作要求未运行测试。没有运行数据库迁移或启动PostgreSQL，不表示`ReadEvidenceContext`SQL已在真实PostgreSQL验证。
- 无百炼Workspace/API Key，没有实际调用qwen3.7-plus-2026-05-26、Function Calling、并行工具拒绝路径、JSON Schema输出或`enable_thinking=false`。官方文档对后者存在直接冲突：Chat API示例说明可设false，而错误码页称qwen3.7-plus不可设false。代码保留此前用户选择的非思考模式，必须账户联调后再定，不静默换模型或改变用户决策。
- 后续先完成M12内容概览/章节作业设计的代码切片，再补迁移与故障/协议验证；最终做删除GC和生产运行配置review。当前数据库、外部服务、视频播放定位和真实SSE断连体验都没有运行证据。

## 后续记录模板

- 日期 / 里程碑：
- 变更文件与行为：
- 设计不变量核对：
- 自动测试与数据库证据：
- 外部联调与真实参数：
- 发现的问题 / 修订：
- 下一步：


## 2026-11-10：M12 内容概览与章节作业（代码已写，待数据库/百炼验证）

### 实现

- 新增独立内容任务和版本模型。TEXT结果在PUBLISH事务中创建revision 1 job与内容head；NO_SPEECH结果不发内容任务。显式重建按`client_request_key`幂等创建revision，固定result、workspace、模型和profile。
- 内容Worker按workspace领取有租约任务，与解析Worker及在线问答隔离。基于不可变转写字幕生成确定性范围计划；最多12批，每批最多100个范围，单范围不超过2048字节，超过预算的尾部明确记录为partial。
- 每批调用预算先持久预留，提取输出严格解码并校验引用；合并阶段只能引用通过提取校验的来源。模型HTTP调用在数据库事务之外，结果通过attempt、epoch和状态围栏保存，避免持锁等待远端模型。
- 最终产物保存概览、章节、覆盖状态和CREF来源映射；来源范围按UTF-8字节在原transcript中逐一核对。内容artifact不可变，只有较高revision完成才推进内容head，生成中的新版本不遮蔽旧产物。
- 新增内容查询/重建/任务lookup/API与`000011_content.sql`。摘要、章节模型输出为不可信输入，不允许工具调用或凭空补充来源外内容。

### Review记录与设计取舍

- 区分核心解析`task_version`、内容`revision`与单次执行epoch：解析结果可固定回答；重建内容不需要重新跑转写/向量，也不会改变核心结果版本。
- 采用固定字幕范围和有界批次，使重试、恢复与来源校验基于持久输入；不让模型自由摘要整段长字幕，以免成本、超时和上下文大小随音频长度失控。
- partial是有意的首版预算结果，API应表达覆盖不完整；不能把未处理的字幕说成已摘要。质量评估集、覆盖率指标与后续续跑策略仍需M13 review。
- 当前为单独的内容Worker槽，未实现多个模型任务间的账号级RPM/TPM全局调度；也未实现内容artifact随视频删除的保留根和GC联动。

### 验证证据与缺口

- 执行`gofmt`和`go build ./...`通过。未运行测试、未执行`000011_content.sql`、未启动数据库，也未调用百炼；因此不能声称SQL、并发、远端Schema能力或内容质量已经验证。
- 首版目前可由代码构建，但真实端到端仍需要PostgreSQL迁移/SQL检查、Fun-ASR已有链路、百炼Qwen响应、失败恢复和内容效果评估。`enable_thinking=false`与所选快照的供应商兼容性沿用M11待验证项。

### 面试问题

**为什么内容生成要建独立job/revision，而不是写进解析任务或问答请求？** 内容总结是可重试、可独立失败的派生能力；把它从核心转写结果和在线问答分开，既可复用固定转写，也能在重建期间继续提供旧版本内容，不让内容模型故障拖慢问答。

**为什么先保存范围计划和调用预算，再请求模型？** 远端调用可能超时但实际已受理或计费。先持久预留能在进程崩溃后计算已消耗的重试预算；固定范围计划让恢复不会因算法或排序变化而重复/遗漏不透明地改变输入。

**如何证明摘要/章节引用确实来自原字幕？** 模型只返回稳定来源ID和片段范围；Go检查ID必须来自已验证提取结果，最终再按UTF-8字节区间与固定transcript逐字节比较，保存不可变CREF映射。模型自己生成的时间戳或引用文本不能作为事实来源。

**处理超长字幕时怎么办？** 首版执行显式12批预算，超出范围标为partial；API必须呈现覆盖状态。后续可以分代续跑，但要讨论单revision多代、来源合并与旧产物可读语义，不应悄悄截断后声称完整。

**为什么不在数据库事务里调用模型？** 模型延迟和失败不可控；保持事务短可降低锁持有和连接占用。调用预算、job租约、epoch和原子发布用于隔离重复调用与并发提交。

**哪些证据还没有？** `go build`只证明代码编译；迁移未执行、SQL竞争未在PostgreSQL验证、没有真实百炼响应与质量集，所以不能把设计正确性或模型效果等同于构建成功。

### 下一步

M13优先review视频/会话删除与对象GC、播放定位、全局供应商预算、可观测性和故障场景，再区分哪些可在本地数据库验证、哪些依赖云账户/FFmpeg；所有运行证据按层记录。


## 2026-11-10：M13 视频删除和延迟回收首片（代码已写，待PG/OSS验证）

### 实现

- 新增`DELETE /v1/videos/{id}`与`GET /v1/video-deletions/{deletion_id}`。受理事务按user→video加锁，把资源改为DELETING、清除生效结果指针、取消活动解析/内容作业、打断运行中问答、归档会话并阻止新的业务读取/受理。
- 运行中的parse/content job执行epoch递增并失去lease；旧worker不能凭旧epoch发布新结果。问答记录先持久化为INTERRUPTED，生成结果不能覆盖终态。
- 取消/完成中的multipart上传按30分钟窗口收敛；删除worker在24小时安全窗口后再扫描上传会话和已登记blobs，逐个调用OSS `DeleteObject`，外部删除结果不明时安全重试。
- 新增`video_deletion_jobs`和`video_gc_objects`，记录phase、lease epoch、对象版本、尝试状态；worker租约接管后重放未确认对象删除。数据库回收按FK依赖清除问答证据、内容版本、结果、向量、chunks、字幕、ASR/blob等；视频行变为DELETED，保留少量幂等墓碑并脱敏问题和上传对象键。
- 新增已验证问答证据的播放URL签发接口：owner/question/evidence/video/source blob或active content artifact/CREF/source transcript联合授权，版本固定，TTL按设计为2小时，并返回原字幕起止毫秒；两类引用互斥，不允许客户端指定URL或对象键。
- 若ASR提交/远端任务仍处于SUBMITTING、UNKNOWN、ACCEPTED、RUNNING或RESULT_READY，删除任务保持WAIT_UPLOADS，不把本地任务取消视作远端已终止。

### Review发现与保留边界

- 删除先逻辑不可见，再异步物理清理；202表示已受理，不能宣称字节已经消失。删除过程以video生命周期作为新引用/受理围栏。
- 删除OSS对象前等待24小时以覆盖签名窗口和本地媒体worker最长执行时间；Multipart另有30分钟收敛窗口。已记入blobs和upload_sessions的对象具备持久化删除账本。
- 当前没有OSS ListObjects/版本清单前缀扫描。因此若旧worker在DELETING后才完成PutObject、且无法写回blobs，孤立对象不会进入本次删除清单。不能宣称达到完整对象擦除；后续需加入按`video_id`前缀和上传session键核对的孤儿清扫。
- 远端ASR处于非终态时，M13 worker选择等待，但现有ASR运行器不会在本地任务被取消后持续独立查询供应商；该清理可能停留在WAIT_UPLOADS/RETRY_WAIT，需要增加远端对账或人工核实路径。
- 当前数据库依赖按视频一次性事务删除大体量明细，不符合最终“小批次避免长事务”的目标；应拆成阶段位点/批次推进。对象状态标记和GC SQL仍需PG验证。

### 验证证据与缺口

- `gofmt`与`go build ./...`通过；没有运行测试、没有执行`000012_video_deletion.sql`、没有连接PostgreSQL/OSS，也没有观察并发取消、对象版本删除或签名URL窗口。
- 问答证据及内容CREF播放URL已实现签发代码；没有真实OSS播放或时间定位误差验证。账号级RPM/TPM统一限流、Prometheus指标/告警、优雅停机故障注入或评估集仍未实现。README已列出相关API与当前边界。

### 面试问题

**删除接口为什么返回202？** 请求事务只同步完成逻辑停用和创建GC作业；OSS删除及大量数据库记录回收不可放在用户请求事务内。202代表受理，调用方用删除任务ID轮询；只有后台核实完成后才将资源标记DELETED。

**旧Worker如何避免复活已删除视频？** API将video.lifecycle设为DELETING，并递增活动任务run_epoch/清除租约；关键阶段发布会检查活动视频和epoch。提交旧epoch失败。对已经写到OSS但未记入数据库的对象仍需前缀清扫，因此当前代码不能宣称覆盖所有迟到产物。

**为什么不马上删OSS？** 预签名请求和正在执行的Multipart/FFmpeg/ASR可能仍在途。数据库先关授权，再等安全窗口、确认上传收敛并保留每个已知对象的持久化GC账本，降低晚到请求造成重新生成对象的概率。

**未知ASR任务怎样处理？** 不将本地取消等同于供应商已停止。当前GC会保留任务并暂停删除；补充的后台对账/人工核实流程仍未实现，这是当前可用性和删除完成时效的明确缺口。

**这轮算完整实现删除了吗？** 代码实现删除受理、围栏、已登记OSS对象删除和数据库脱敏/清理首片；未实现孤儿前缀扫描、未知ASR恢复、分批SQL和实地验证，因而属于M13首片，不是生产验证完成。


## 2026-11-10：本地离线问答纵向演示

### 实现

- 增加显式关闭的`LOCAL_DEMO_MODE`。启用时要求HTTP监听地址、PostgreSQL URL和所有Kafka broker均为回环地址，并拒绝配置OSS bucket或百炼workspace/API key，降低把本地夹具误接到远端服务的风险。
- 新增`cmd/seed-demo`，事务性幂等写入固定视频元数据、两条字幕、两条chunk、两个1024维单位向量和task_version 1的TEXT结果清单。固定数据没有真实源视频对象，种子重复运行会验证内容/数量，不覆盖不一致的已有夹具。
- 新增确定性本地Embedding与回答器。问答profile记录`local-demo-fixture`和fixture model名称，不伪装成text-embedding-v4或Qwen调用；最终文本显式带“本地离线演示固定答案”。
- 通过该夹具走真实API和本地PostgreSQL检索；固定Agent先发起一次`search_video`，再因没有新增证据而停止补查并收尾，因此覆盖工具参数校验、工具处理、证据登记、严格答案/引用校验、答案落库及SSE发送。

### 验证证据与缺口

- `gofmt`、`go build ./...`和`git diff --check`通过；按当前要求未运行测试。
- API启动时PG17/pgvector记录的迁移数为12，即`000001`至`000012`全部已应用。当前fixture v2的`seed-demo`首次执行和重复执行都成功并返回相同video_id=`28ed80a6-348e-5bf2-91a9-065e587f994e`；更早fixture v1数据保留，没有原地改写已发布结果。
- 本地API验证登录201、创建对话201及幂等重放200、提问SSE 200；事件包含accepted、progress（含`agent_searching`）、answer、done。问答状态为SUCCEEDED，数据库中有2条证据、5个question_steps（含`TOOL:ok:search_video`），`generation_profile.provider=local-demo-fixture`。回答已在DB保存后经SSE返回。
- 单位向量只为固定夹具的两条语句建立确定性检索路径；应用查询和来源映射可运行，但不证明通用语义召回。搜索工具这次没有新增证据，因此Agent按no-progress规则停止补查；生成回答仍是固定文本，不证明Agent决策质量。没有调用真实百炼，也没有验证`read_context`、上传、OSS、FFmpeg、Fun-ASR、真实Embedding/Qwen、内容Worker或删除GC。

### 面试问题

**离线夹具能证明什么，不能证明什么？** 它能证明API、数据库事务、版本固定、证据登记、回答校验、持久化和SSE在本地串通；不能证明供应商协议兼容、媒体识别质量、语义召回质量或生成质量。profile必须明确写本地夹具，避免账单/观测把mock统计成云调用。

**为什么固定向量不能证明RAG质量？** 夹具用预设单位向量路由到固定句子，没有文本语义；它验证的是pgvector SQL、工具链和来源映射。召回质量需要人工标注的问题/证据集和真实embedding，另报Recall@K等指标。


## 2026-11-10：M13数据库回收改为持久化批次

### 实现

- `000013_video_gc_batches.sql`扩展视频删除job的phase约束，并将历史`PURGE_ROWS`状态迁移到`CLEAR_VIDEO_REFS`；`000014_video_gc_indexes.sql`新增16个按video及清理谓词定位的索引。API启动时已经在本机PostgreSQL 17/pgvector应用到000014，共14份迁移。
- `PurgeBatch`把视频引用冻结、问答/内容/解析元数据脱敏与FK依赖清理拆成持久化阶段。每个事务最多处理500行；当阶段清空时在同一事务推进phase，最终单独FINALIZE视频墓碑和删除任务。
- 删除worker每次领取最多完成25个数据库批次后让出租约并重新排队；OSS对象删除也按有限批次让出。worker接管后从数据库保存的phase继续，不依赖进程内游标。

### 验证证据与缺口

- `gofmt`与`go build ./...`通过；000013/000014迁移已实际应用，确认16个索引存在且旧`PURGE_ROWS`不再是运行期phase。
- 未执行删除任务；没有实际跑批量清理SQL、租约接管或中断恢复，也未做OSS验证。因此仍不能排除运行时SQL、FK顺序和批次谓词问题。
- 孤儿对象前缀扫描、未知ASR远端状态对账、跨任务全局模型配额、运行指标/告警与删除故障演练仍未完成。
- 本轮未运行测试套件。

### 面试问题

**为什么GC需要固定小批次？** 一次删完所有视频数据会让事务时长、锁范围、WAL与回滚成本随数据量增长。固定批次控制单次影响，但要付出阶段账本、幂等谓词和最终完成屏障的复杂度。

**为什么phase推进与清理SQL同事务？** 让恢复点与本批数据库副作用原子一致：事务失败则二者一起回滚；事务提交则可以从下一个阶段继续。

**为什么worker每25批让出？** 防止一个大视频长时间垄断后台执行机会；让租约和调度公平性有界，并降低关闭/接管等待。让出期间的阶段保存在数据库中。

**现在能说删除完成了吗？** 只能说批处理实现已编译、数据库迁移已应用；没有真正执行删除SQL或OSS调用，孤儿对象与未知ASR处理也未完成，不能声称删除链路验证完毕。


## 2026-11-10：M13孤儿OSS对象发现与版本回收

### 实现

- 上传源视频对象键改为`source/{video_id}/{upload_session_id}/{nonce}`；音频与ASR结果键已有video_id分区。删除Worker按三个固定前缀列举OSS对象版本与删除标记，补充仅查数据库账本可能遗漏的已上传未登记对象。
- OSS适配器一次请求读取最多500个版本条目。`video_deletion_jobs`新增扫描prefix/key/version游标；每页发现版本、写入`video_gc_objects`与保存游标位于同一事务。扫描重放由对象身份唯一索引去重，旧phase为DELETE_OBJECTS的未完成job在迁移时改为初始扫描。
- 新增`000015_video_gc_discovery.sql`。删除流程在安全等待后执行初始扫描、对象删除、数据库批量清理，再做最终扫描；最终扫描发现对象后再删除并清空对应账本，最后Finalize。每次领取最多处理25页或25个对象后让出。
- 前缀限定为`source/{video_id}/`、`artifacts/audio/{video_id}/`和`artifacts/asr/{video_id}/`。历史上传键没有video_id段，已知源对象仍从upload_sessions/blobs登记；不对整个owner或bucket无边界扫表。

### 验证证据与缺口

- `gofmt`、`go build ./...`及`git diff --check`通过。API启动成功并应用`000015_video_gc_discovery.sql`；本机数据库共15份迁移，检查到扫描游标列和扩展phase约束，`/healthz`返回ok。
- 当前无阿里云凭据，没有实际执行OSS版本列举/删除，没有运行删除Worker或验证批处理SQL、权限范围、Bucket版本控制语义和外部写入竞争。不能据此宣称OSS孤儿清理已运行验收。
- 一次最终扫描能覆盖正常最大执行窗口内的迟到对象，但不能对无限期失控进程作绝对承诺；该设计依赖24小时等待、run_epoch取消和有限Worker调用窗口。旧格式、且数据库完全失去登记关系的源对象不在video_id前缀下，扫描无法关联。
- 未运行测试套件。
- 后台重启时发现Kafka consumer启用了`BlockRebalanceOnPoll`却调用普通`Close()`，在已有poll后退出会等待最后的revoke。改为`CloseAllowingRebalance()`；本地background收到SIGINT后快速记录shutdown并正常返回。

### 面试问题

**数据库外的OSS副作用如何纳入删除？** 对象写入先使用video_id隔离前缀；GC将数据库引用快照与OSS版本枚举合并进同一持久化账本，再按version ID执行可重放删除。数据库不能跨OSS事务，所以依靠外部副作用账本、幂等重试和最后核对。

**游标和发现对象为什么在同一事务提交？** 先提交游标会有崩溃丢失窗口；先提交对象再提交游标会重复扫描。把账本插入和游标推进原子提交，重放时由唯一索引消除重复。

**M15运行到什么程度？** Go构建成功、数据库迁移实际应用；没有云凭据，因此ListObjectVersions/DeleteObject和真实GC链路未执行验证。扫描逻辑仍需在专用OSS bucket做版本开关、权限、重复扫描、分页接管和迟到写入演练。


## 2026-11-10：M13/M16已知Fun-ASR任务远端对账

### 实现

- 新增独立`internal/background/asrreconcile`后台worker和`ASRReconciliationRepository`。仅处理parse task已取消或video已进入DELETING/DELETED、且provider task ID已知的任务；持久化租约使用epoch/owner/lease_until围栏，避免多实例重复处理。
- 对`GetTask`的成功响应分类为非终态或已知终态。已知终态持久化为`REMOTE_TERMINAL`；非终态更新provider状态、poll计数和下次查询时间。查询错误持久化错误码和指数退避；404有较长退避但仍不解除删除门禁。
- 提交HTTP响应和视频删除竞态时，接受/明确拒绝结果现在按独立submission ID落账，避免parse lease已撤销导致task ID丢失；UNKNOWN结果会先提交账本，只有原run_epoch/lease仍有效才把parse task改为WAITING。迟到的可信接受结果可将UNKNOWN更新为ACCEPTED。
- 删除等待原因区分`UPLOAD_TERMINATION_PENDING`、`ASR_REMOTE_RECONCILIATION_PENDING`和`ASR_TASK_ID_UNAVAILABLE`，视频删除状态API返回`blocking_reason`。无task ID的UNKNOWN作业不会自动释放或猜测云端已终止。
- 新增`000016_asr_reconciliation.sql`：增加reconciliation租约字段、attempts、remote_terminal_at及`REMOTE_TERMINAL`状态和due索引。

### 验证证据与缺口

- 执行`gofmt`、`go build ./...`和`git diff --check`通过。API启动将本地PostgreSQL 17/pgvector迁移到000016。没有运行测试套件。
- 未配置真实百炼或OSS，故未调用GetTask、OSS版本枚举/删除或真实删除Worker。没有用PostgreSQL验证任务取消与ASR响应竞争，也没有检查具体供应商真实终态返回形态；不能把构建/迁移成功表述为远端流程验收。
- UNKNOWN无task ID仍需人工或供应商侧核实；代码没有远端取消能力，也没有通过固定超时释放删除门禁。

### 面试问题

- **为什么远端对账要脱离解析Worker？** parse task被用户取消后，本地run_epoch/lease应该失效，但供应商已有任务仍可能运行。独立对账器以ASR提交账本为输入，可在解析任务结束后继续管理外部副作用。
- **哪个状态可以解除删除等待？** 本地FAILED/CANCELLED不够；需要供应商查询给出明确终态，并把这一事实持久化。这样进程重启和多实例调度不会丢失删除判定。
- **未知task ID怎么办？** 无法按ID查询特定任务，所以保留安全阻塞并提示人工核查。自动重试最多一次用于业务任务推进，不能证明未知的第一次调用没有在供应商侧成功。
- **为什么404不放行？** 404只说明当前查询接口未找到/不可见，不能排除workspace、地域、权限或保留策略差异，因此仍按未确认处理。


## 2026-11-10：M17 百炼模型共享RPM/TPM准入

### 实现

- 由于API与background都会调用百炼Embedding/Qwen，新增数据库共享协调而非各进程独立令牌桶。`provider_quota_gates`按workspace/model保存RPM、TPM和429冷却；`provider_call_reservations`按每次HTTP尝试保存预留token、供应商usage、request ID与结果码。
- 每次准入在短事务锁住一个gate行，统计过去60秒的调用数及token消耗；通过后插入请求预留并提交，然后再发HTTP。失败同样消耗一次RPM并保留token估算；成功返回usage后用另一个短事务修正预留。不会持有数据库锁等待模型。
- Qwen适配器按输入JSON字节、最大输出及Schema/tool开销保守估算；Embedding按输入UTF-8字节和少量协议余量估算。没有将这些字节数称为精确token。供应商429会将Retry-After写入同一gate，阻止另一个进程立即继续撞限流。
- 配置引入`BAILIAN_EMBEDDING_RPM/TPM`和`BAILIAN_QWEN_RPM/TPM`。启用百炼必须填写实际账号额度，范围受基本健全性约束；API和background值不一致、与数据库gate已存配置不一致都会拒绝启动。不存在默认虚构的供应商额度。
- 新增迁移`000017_provider_quota.sql`与`internal/providerquota`生产适配器；命令入口统一包装所有实际Embedding与Qwen调用。

### 验证与缺口

- 2026-11-11复核：执行`gofmt`、`go build ./...`与`git diff --check`通过，未运行测试。新API启动后确认本机PostgreSQL记录17份迁移，`provider_quota_gates`与`provider_call_reservations`存在；8080 healthz/readyz通过，API和background均已用当前代码启动。未调用百炼/OSS。
- 当前没有阿里云凭据与控制台实际额度，因此没有创建实际模型gate配置、发送模型请求或验证usage/Retry-After。也没有模拟双进程竞争、长等待取消、窗口公平性或压力。
- gate scope目前是一个Workspace中的每个模型；若同一阿里云账户多个Workspace共用一份配额，workspace key会把它们分开，需要运维指定并实现公共quota_scope。共享数据库成为配额保护依赖；数据库不可用时模型调用失败关闭。
- 滑动60秒窗口限制总量，但不是严格的平滑每秒token bucket；额度内允许短时突发，供应商更细的并发/秒级限制仍通过429共享冷却处理。

### 面试追问

- **为什么门闩在数据库？** API和background进程各有自己的内存，局部互斥无法约束共同账号；DB gate行锁提供跨进程原子预留，而且只持有短事务。
- **为什么重试也要预留？** 每一次HTTP重发都可能被供应商接收并计费；逻辑上的一次批次不能只算一次RPM。
- **TPM估算不准怎么办？** 先保守预留，成功后按usage回填，失败且结果不明时不退款；缺少tokenizer意味着这是上界近似，不是精确限制。实际429仍需共享冷却与告警。
- **这个限流是严格全局账号限流吗？** 只对本应用配置的单workspace/model全局；多个workspace若共享账户配额还需要公共scope，部署多个API/background的前提是都连接同一PostgreSQL并使用相同配置。


## 2026-11-11：M18 Prometheus基础运行指标

### 实现

- 新增`internal/observability`，每个进程创建独立Registry，注册Go runtime/process collector并以`promhttp.HandlerFor`提供`/metrics`。
- API与background分别用`API_METRICS_ADDR`、`BACKGROUND_METRICS_ADDR`监听；默认地址为`127.0.0.1:9091`和`127.0.0.1:9093`。LOCAL_DEMO_MODE会拒绝非loopback指标地址。metrics listener有独立的超时和优雅停机流程。
- HTTP请求指标使用Gin route template；provider指标覆盖Embedding/Qwen调用量、耗时、供应商usage/估算回退、配额准入、等待、预留token和quota账本写入失败；worker指标覆盖media/content/deletion阶段耗时、结果和活动数。
- 标签采用白名单固定枚举，不放user/video/task/request ID；未知route为`unmatched`，未知模型或stage为`other`。
- 删除和内容/媒体Worker以可选指标依赖构造，现有调用点保持兼容；模型provider quota adapter通过observer接口记录调用和准入，不改变数据库门闩语义。

### 验证边界

- 执行`gofmt`、`go build ./...`和`git diff --check`通过；未运行自动化测试。
- 2026-11-11用M18新代码重启API/background；API healthz/readyz通过，9091/9093的`/metrics`可访问，API请求计数含healthz/readyz，background Go runtime/process指标有样本。quota账本错误指标已编译并随当前进程启动，但无provider调用，不应有该样本。9092由Kafka占用，因此background默认端口改为9093。
- 尚无Prometheus server配置/告警规则；未采集Outbox/Inbox lag、待处理队列年龄、连接池等待、ASR/删除阻塞数、临时盘水位，也没有真实provider流量或告警演练。
- 默认指标监听只绑定本机回环地址；内网部署可覆盖配置，但应通过网络策略限制scrape访问。Prometheus Go client使用方式参考[官方接入指南](https://prometheus.io/docs/guides/go-application/)。


## 2026-11-11：M19 Prometheus抓取、告警与数据库队列观测

- Compose新增Prometheus `v3.16.0`服务，监听仅绑定本机`127.0.0.1:9090`，保留7天；新增`monitoring/prometheus.yml`每15秒抓取API `9091`和background `9093`，并挂载告警规则。
- API/background导出pgxpool连接数、连接获取、空池等待、取消与连接创建/回收数据。background额外执行有3秒超时的单条数据库聚合查询，报告parse task、Outbox、content job、video deletion、ASR remote状态数量、最老到期年龄、过期租约和远端deadline；采集失败通过独立指标呈现。
- 告警规则扩至16条，覆盖target down、API 5xx、连接池饱和、明显连接池等待取消、provider 429/配额等待/账本写失败、Worker阶段失败、队列查询失败、parse/outbox/deletion积压、failed outbox、过期租约、ASR deadline与无task_id的UNKNOWN。
- 验证：`gofmt`、`go build ./...`、`podman compose config`通过；容器内`promtool check config`通过并识别16条规则。重启background后，Prometheus API显示API/background target均为UP；queue snapshot错误为0、耗时约6ms，pgxpool指标存在。数据库没有活动队列记录，因此没有任务状态非零样本，未演练告警触发。未运行自动化测试。
- 风险/边界：队列聚合按15秒scrape频率访问数据库，生产规模要观察索引和池开销；阈值还不是基于SLO/压力数据校准。没有真实百炼/OSS流量、故障演练或通知渠道。
- 面试问题：Prometheus的HTTP scrape成功不等于采集器内业务查询成功，因此单独导出`scrape_error`；队列健康应从PostgreSQL事实状态派生，不能靠进程内计数；指标标签保持低基数，不带任务/用户ID。


## 2026-11-11：M20 Parse Task控制API与OpenAPI契约

### 实现

- 新增`GET /v1/parse-tasks/lookup?client_request_key=...`、`POST /v1/parse-tasks/{id}/retry`和`POST /v1/parse-tasks/{id}/cancel`。lookup按当前owner和原创建键返回任务；写接口仍要求会话Cookie与CSRF。
- 人工继续使用独立`Idempotency-Key`，在user→video→task锁序的短事务内核验一次性上限、任务为可恢复失败、源blob仍有效及用户活动任务额度。写`parse_task_retry_history`与重置可重试task_step、恢复同一task的READY状态一并提交；同一个`task_version`继续，保留产物检查点和ASR提交账本，不重置外部预算。相同请求键可重放；不同键再次申请返回耗尽冲突。
- 取消将活动parse task改为`CANCELLED`并递增`run_epoch`，清除lease，使迟到Worker提交失效；重复请求返回当前终态。此接口不声称取消Fun-ASR远端任务，已知远端task ID继续由独立reconciler对账。
- `GET /v1/parse-tasks/{id}`和lookup返回有限stage progress、失败码与`retry_allowed`；不返回供应商raw error或签名数据。阶段进度从持久化task_step与字幕/片段/向量提交量计算。
- 对齐`GET /v1/videos`与`GET /v1/videos/{id}`中的`latest_task.retry_allowed`：二者使用同一源blob有效、任务失败类型及人工继续次数条件，避免列表摘要因默认零值错误隐藏恢复操作。
- 新增`000018_parse_task_controls.sql`、`internal/httpapi/openapi.yaml`，并从`GET /openapi.yaml`嵌入式提供OpenAPI 3.1文档。API写超时为210秒，覆盖180秒问答业务deadline及终结SSE事件；读超时为30秒。

### 验证证据与限制

- 执行`gofmt`和`go build ./...`通过；Ruby YAML解析器成功读取OpenAPI文件。API启动将本机PostgreSQL迁移应用至`000018`；`/healthz`、`/readyz`和`/openapi.yaml`返回200。
- 使用已有离线演示账户请求既有成功task：`GET /v1/parse-tasks/{id}`返回200，含PUBLISH阶段1/1进度；终态任务cancel返回200且无状态变更；对成功任务调用retry返回409 `TASK_NOT_RETRYABLE`。之后用临时video/blob/FAILED task构造可恢复任务验证真实HTTP事务：首次retry返回202，同一`Idempotency-Key`重放200并带`Idempotency-Replayed: true`，不同key返回409 `MANUAL_RETRY_EXHAUSTED`；数据库确认status=READY、manual_retry_count=1、task_version仍为1、ASR step=PENDING且failure_count=0、审计表有1条记录。临时数据均已清理。
- 新一轮API重启后，视频列表和视频详情都返回200；本地SUCCEEDED夹具的两处`latest_task.retry_allowed`均为false，与任务详情一致。
- 运行期第一次查询发现`stage_progress`的SQL简单CASE误用了布尔`WHEN`，导致PostgreSQL `text = boolean`。改成嵌套条件CASE后，同一真实HTTP查询返回200；这条故障发现与修复已记入本日志。
- Prometheus当前仍显示API和background target均为UP；未运行自动化测试、形式化OpenAPI validator、长时SSE超时验收或真实OSS/百炼调用。人工继续使用合成数据库行证明API/事务可执行，不代表真实媒体/ASR产物恢复完整，也未模拟并发状态冲突。

### 面试问题

- **为什么`task_version`和`run_epoch`继续分开？** 人工继续是同一逻辑配置与业务版本，Worker重新领取才提升执行epoch；取消提升epoch用来围栏旧写入。只有用户请求不同配置或创建新业务轮次时才分配新的`task_version`。
- **如何避免人工重试重复建任务或重复扣ASR预算？** 重试键与任务行在一个事务内持久化，单任务最多一次；同键重放读回同一结果。沿用原ASR submission ledger，所以Worker接管/人工继续都不能把provider提交计数清零。
- **本轮验证能证明什么？** 构建、迁移与合成FAILED任务HTTP执行证明查询、事务受理、同键幂等、次数上限、阶段计数重置和审计写入可运行；它不证明真实媒体/ASR外部产物能成功恢复，也未覆盖并发冲突。
- **API写超时为什么210秒？** 问答应用deadline是180秒，HTTP再保留30秒发送已持久化的最终SSE事件；ReadTimeout独立设30秒。反代还需按SSE语义配置更长的响应窗口并关闭缓冲。


## 2026-11-11：M21 取消阶段状态与执行围栏

### 发现与实现

- 本地活动任务取消检查发现父parse task虽然已标记CANCELLED并增加run_epoch，当前`task_steps`却仍为RUNNING，任务进度接口因此呈现矛盾状态。
- 新增`000019_task_step_cancellation.sql`，将task_step状态约束扩为允许CANCELLED。`CancelParseTask`在同一个事务撤销任务lease、递增run_epoch，并把当前阶段PENDING/RUNNING checkpoint更新为CANCELLED，记录`TASK_CANCELLED`元数据；已有ASR submission关联保持不变，供独立远端reconciler查询。

### 验证与边界

- `gofmt`和`go build ./...`通过。重启API后迁移已应用，本机PostgreSQL记录到`000019`，约束检查包含CANCELLED。
- 用临时RUNNING任务经HTTP取消：返回202，task和ASR checkpoint均显示CANCELLED；模拟Worker使用旧run_epoch=7更新，PostgreSQL返回UPDATE 0，任务保留CANCELLED/run_epoch=8。临时video/blob/task/step已清理。
- 未运行自动化测试或真实Worker goroutine竞争；epoch检查仅为数据库条件更新级验证。没有OSS或Fun-ASR凭据，不表示远端操作取消或外部文件已回收。

### 面试问题

- **为什么父任务和阶段检查点要一起取消？** 两者回答不同问题：task状态给用户看，task_step说明在哪个阶段。只改父状态会留下RUNNING进度假象，所以同一事务同步终止当前活动checkpoint。
- **旧Worker如何被挡住？** 所有阶段写入核验task状态、run_epoch、owner和lease。取消先提交后，旧执行的旧epoch条件更新影响0行；task行协调提交顺序。
- **取消是否终止Fun-ASR？** 不代表。取消撤销本地解析执行资格；submission ID与状态保留，独立reconciler等供应商明确终态后再解除外部副作用相关门禁。

## 2026-11-11：M22 删除回收数据库批次运行核对

- 用独立临时Go harness直接连接本机PostgreSQL，创建无真实业务数据的DELETING视频、删除作业及501条`DELETED`状态GC账本记录。调用真实`VideoDeletionRepository`的领取、依赖检查、对象快照、扫描位点提交和`PurgeBatch`方法；用三组空OSS前缀页模拟初次/末次版本发现，不调用OSS或Fun-ASR。
- 验证`PURGE_GC_OBJECTS`第一事务后剩余1条、阶段保持不变；第二事务删除最后1条并前进到末轮扫描，确认500条上限生效。数据库清理阶段共完成31次PurgeBatch调用；最终视频`lifecycle=DELETED`、job=`SUCCEEDED`、GC账本为0。
- harness的defer路径清理临时用户、视频、删除job与账本；另查数据库四类临时记录均为0；临时源文件已删除。
- 这证明数据库清理阶段和500+1边界可在本机PostgreSQL运行，不证明有数据量下的锁/性能，也未覆盖带完整内容图的FK删除、Worker循环/租约竞争、OSS真实版本分页或外部删除。演练的临时job立即可执行，绕过正常24小时保护期；因此不代表实际删除时序验收。
- 未运行自动化测试；`go build ./...`此前已通过，本轮只运行临时手工数据库harness。

### 面试追问

- **为什么删除分500行事务？** 限制单个事务的锁持有、WAL和回滚范围；每个批次与phase推进原子提交，进程崩溃后从数据库保存的phase继续。当前演练证明501条GC账本被拆成500+1，不代表已压测生产负载。
- **为何清理后还要末轮OSS扫描？** 初次清单后，已失租或迟到的旧执行可能产生对象；在延迟清理和本地执行epoch围栏之后再扫一次，发现并删除在途窗口内的对象。当前仅验证空页游标与数据库状态，没有真实OSS结果。

## 2026-11-11：M23 视频删除时同步阶段取消

- Review发现`DeleteVideo`会将活动parse task改为CANCELLED并增加`run_epoch`，但仍保留PENDING/RUNNING的`task_steps`。视频进入DELETING后用户不能读取详情，但数据库队列与stage状态仍可能错误显示阶段在运行，语义与M21单任务取消不一致。
- 在视频删除同一PostgreSQL事务内，先取消视频下活动parse task，再将其PENDING/RUNNING task_step设为CANCELLED，写`VIDEO_DELETED`错误码、完成时间与更新时间。复用`000019`新增的状态值，不需要新迁移。
- `gofmt`和`go build ./...`通过。用临时ACTIVE video、RUNNING ASR task（epoch 7）和RUNNING ASR step调用真实`ResourceRepository.DeleteVideo`；核对task为CANCELLED/DONE、epoch=8，step为CANCELLED、错误码`VIDEO_DELETED`且完成时间已写入。临时用户/video/task/step/deletion job均清理，数据库计数为0。
- 未运行自动化测试、真实Worker并发、OSS或ASR；单次临时数据库执行不构成压力/竞态验收。

### 面试追问

- **为什么删除视频时也要更新task_step？** task是业务状态，step是恢复与监控进度；只取消父task会留下运行中假象。由于二者在同一事务变更，删除后不会出现可见任务已取消但阶段还在执行的矛盾。
- **为什么不清空当前stage即可？** stage字段表示任务状态机位置，step是持久化执行记录；改父字段不会撤销checkpoint本身。需要在事务中分别更新，并让旧Worker受到run_epoch/lease围栏。


## 2026-11-11：M24 ASR UNKNOWN无task_id审计运维处置

### 实现

- 对`UNKNOWN`且无`provider_task_id`的submission，已知task_id的reconciler无法继续查询，删除job必须保持`ASR_TASK_ID_UNAVAILABLE`等待；不能把超时当供应商终态。
- 新增`000020_asr_manual_resolution.sql`和`asr_manual_resolutions`，持久化操作者、确认的供应商终态、证据引用、原因、处置时间及是否被迟到受理取代。
- 新增私有Go命令`cmd/asr-unknown-resolve`。它不会调用供应商，只接受带显式确认短语的人工核实；要求operator、terminal status、evidence ref和reason。DB凭据是运维边界，证据引用不得包含签名URL。
- 仓储使用user→video→parse task→submission锁序，仅允许task已取消或video正在删除时处理UNKNOWN/no-task-id记录。审计插入、`REMOTE_TERMINAL`状态更新、错误清理与reconcile lease清理处于同一事务。完全相同且未被取代的命令重放返回成功，不重复审计。
- `RecordASRAccepted`处理迟到的可信task_id时，会把人工终态恢复为ACCEPTED、清除`remote_terminal_at`并以`LATE_PROVIDER_ACCEPTANCE`标记旧人工处置被取代，交还独立reconciler。

### 验证与限制

- `gofmt`与`go build ./...`通过；临时PostgreSQL fixture实际运行CLI两次，确认命令幂等且只有一条审计；再调用生产仓储`RecordASRAccepted`，确认迟到受理恢复ACCEPTED并supersede旧审计。测试账户、视频、task、blob、submission和audit均清理。
- 本地迁移序号为20，真实百炼/OSS仍未接入；没有真实供应商终态、权限分离流程或自动化测试证据。人工命令的事实输入必须来自值班人员已完成的外部核实。

### 面试问题

- **为什么不在五分钟后自动把UNKNOWN当失败？** 远端是否受理与本地观察超时不是同一事实；放行会让任务并发和删除状态低估潜在远端作业。
- **审计为何和状态更新放同一事务？** 若只写状态或只写日志，都会留下无法解释的缺口；原子提交保证每个人工终态都能追到操作者、依据与原因。
- **晚到响应为什么能推翻人工状态？** 人工命令是在“没有task_id”的信息条件下判断；之后出现供应商task_id是新事实。保留旧审计并显式supersede，同时恢复自动对账，比静默覆盖或永久拒收更准确。
- **验证到哪里？** 合成本地记录验证了DB和仓储状态机；没有供应商调用，不能证明云端终态或人工证据核验有效。


## 2026-11-11：M25 Worker退出/租约回收时同步阶段检查点

### Review发现与实现

Review发现parse-task后台维护的租约回收和总deadline终止只更改`parse_tasks`，当前`task_steps`仍可能为RUNNING；background在正常关机时调用的`Manager.Retry`也只释放父任务执行权。由此任务详情/恢复记录会和父任务状态矛盾。

- `Manager.release`改为短事务，先以status/run_epoch/lease_owner/未过期lease条件更新父任务，再把当前RUNNING阶段更新为PENDING。若父级围栏影响0行则返回`ErrLeaseLost`并回滚；阶段输出引用、输入指纹、已提交批次及failure_count不清理。
- `reapExpiredLeases`在同一数据修改CTE中将过期RUNNING任务转为RETRY_WAIT，并把当前RUNNING step退回PENDING，写入`WORKER_LEASE_EXPIRED`。
- `failOverdue`会同时把当前PENDING/RUNNING步骤和parse task标记FAILED，并写`TASK_DEADLINE_EXCEEDED`；前序READY步骤不变。
- 租约丢失不递增阶段failure_count；重获任务时由claim增加run_epoch，阻止旧执行提交。

### 验证与边界

- `gofmt`、`go build ./...`、`git diff --check`通过。临时PostgreSQL harness调用真实`Manager.MaintainOnce`与`Manager.Retry`，观察过期租约RETRY_WAIT/PENDING、总deadline FAILED/FAILED、正常关机release RETRY_WAIT/PENDING；模拟已提交批次数分别为2、3、4，检查恢复前后`output_metadata`未被清空。临时用户/视频/task/step与harness源文件已清理。
- 结束时重启background，API healthz/readyz、background metrics均返回200，Prometheus API上的API/background targets均为UP。未运行自动化测试、多实例Worker竞争、实际goroutine中断或压力实验。

### 面试问题

- **lease过期与业务失败的语义有什么不同？** lease过期只是执行资格丢失，可能由进程或网络故障造成；阶段可恢复，因此父任务重试、step回PENDING，failure_count不增加。
- **为什么状态同步需要事务？** parent决定任务可否被claim，checkpoint决定恢复和可见进度；必须共同提交，避免任务可重领而step还RUNNING，或任务FAILED但进度仍等待。
- **如何保留进度又挡住迟到Worker？** 重试不删除已提交产物和连续位点；新Worker领取时递增run_epoch，所有提交仍校验epoch与lease，旧执行即使完成外部工作也不能提交数据库状态。
- **deadline和lease失效为什么采用不同step状态？** deadline是明确的任务终止条件，当前阶段置FAILED；lease到期只撤销一次执行权，stage本身仍可在后续接管时恢复，所以置PENDING。


## 2026-11-11：M26 并发领取与旧Worker写入围栏核对

用一次性本地Go harness启动两个独立`tasks.Manager`，使用channel闸门同时对同一条READY ASR任务执行`ClaimNextStages`。一次只创建一个task；结果是一方获得epoch 1，另一方没有任务，验证PostgreSQL `FOR UPDATE SKIP LOCKED`在该场景未重复发放。

随后将任务lease置过期并运行真实`MaintainOnce`，step转回PENDING；新Manager领取得到epoch 2。旧Manager用epoch 1执行`Retry`返回`ErrLeaseLost`，`RenewLease`返回false，真实`StageRepository.StartStage`返回`ErrStageLeaseLost`；数据库保留新owner的RUNNING、epoch 2及step last_run_epoch 2。所有临时用户、视频、task、step和源文件已清理。

边界：这是同一本机PG上的双Manager协调运行，不是跨主机网络故障或压力测试；只覆盖一个任务及Retry/Renew/StartStage入口，不能代表每个数据库提交路径均已全覆盖。未运行自动化测试。

面试追问：数据库行锁用于跨进程协调领取，内存mutex不能协调多副本；run_epoch在领取事务之后继续围栏旧执行者。SKIP LOCKED避免领取者互相等待，但不保证公平性或吞吐上限；本次没有测这些性质。

## 2026-11-11：M27 转写阶段从持久化前缀跨epoch恢复

### 实现路径与证据

- 一次性Go harness在本机PostgreSQL建立READY源视频/音频/ASR raw blob和checkpoint，原始ASR JSON包含250条句子。创建BUILDING transcript，按真实schema先写入完整前200条`transcript_segments`并将`last_committed_seq=200`，task处于TRANSCRIPT/run_epoch=7且lease过期。
- 调用真实`tasks.Manager.MaintainOnce`与`ClaimNextStages`，验证task/step进入RETRY_WAIT/PENDING后同task被新执行领取为epoch 8。
- 调用生产`TranscriptProcessor.Process`与真实`StageRepository`。仅对象存储接口由临时fake返回固定字节及与DB记录一致的版本/metadata；Fun-ASR JSON为合成数据。Processor重新验证大小、对象版本、SHA、profile metadata，归一化输入，调用`VerifyTranscriptPrefix`核对已存200句，追加余下50句并原子提交完整READY artifact与TRANSCRIPT检查点，推进task到READY/CHUNK。最终数据库确认250/250句、task_version仍1、epoch 8。
- 临时数据和fake harness已删除。没有真实OSS/Fun-ASR/媒体文件，也未执行自动化测试或多批次并发中止。

### 面试问题

- **怎样防止恢复时跳过或串入错误句子？** 原始输入通过DB blob identity、sha、profile、对象版本和metadata验证；重新归一化后将持久化prefix逐字段对比，再只从连续位点之后追加。
- **为什么使用单调连续位点而不是MAX(seq)？** `last_committed_seq`在批次事务中与全部批明细一起推进；它表达从1开始无空洞的已提交前缀，MAX不能区分中间缺号。
- **重领为什么改变run_epoch但不换task_version？** 同一逻辑解析轮次和输入继续接管，只改变执行授权；现存有效转写可以恢复复用，下游发布仍以业务task_version比较。


## 2026-11-11：M28 CHUNK从已提交前缀恢复

### 实际路径

- 本地临时fixture建立250条READY transcript source句（每句600 UTF-8字节）与到期租约的RUNNING CHUNK任务/step。用生产`chunk.Build`和当前config/counter计算250条预期chunk，生成真实输入/config/counter fingerprint；在BUILDING chunk_set中先插入前100条chunk及每条source ranges，模拟已提交批次。
- 调用任务维护和claim后，run_epoch从9变为10。执行生产`ChunkProcessor.Process`：从数据库读取完整transcript，确定性重算chunks，`PrepareChunkSet`核验输入/config/counter和已有前缀，`SaveChunkBatch`逐批校验已存在chunk/source并补齐后续150条，`CompleteChunkSet`复核完整集合并在同一事务写READY阶段、发布chunk_set并推进任务至EMBEDDING。
- PostgreSQL检查得到chunk_set READY，实际chunk数等于expected=250、task_step CHUNK为READY、parse task为READY/current_stage=EMBEDDING/run_epoch=10。数据库夹具与harness清理，未增加迁移。`go build ./...`和`git diff --check`通过；未运行自动化测试。

### 面试问题与限制

- **恢复时为什么信任前缀之前要重算？** BUILDING的部分rows不能仅凭存在就认作有效。固定的transcript fingerprint、配置、counter与重新生成的chunk文本、hash、时间、UTF-8字节数和source ranges都要一致；写批次会验证冲突而非盲目覆盖。
- **chunk身份为何不使用run_epoch？** 稳定身份绑定artifact和seq；lease epoch变化不改变同一输入与profile的逻辑产物。
- **证据证明什么？** 实际Processor和PostgreSQL运行从100/250完成至250、发布并推进下一stage；输入为合成字幕。没有真实模型、媒体、生产数据、Worker中断竞态或大规模性能证据。


## 2026-11-11：M29 Embedding批次账本恢复

### 实际路径与证据

- 在本地PostgreSQL建立12条合成chunk输入及过期租约的EMBEDDING任务，task处于run_epoch=11。embedding_set预置为BUILDING：第0批10条已经SUCCEEDED且向量已落库；第1批2条停留在IN_FLIGHT、attempts=1，模拟调用结果未能确认的中断窗口。
- 运行真实任务维护/claim后接管到epoch 12，再调用生产`EmbeddingProcessor.Process`与`StageRepository`。处理器重验集合输入指纹、记录指纹与成功批次向量；已成功批次不再调用provider。遗留批次按账本重试，attempts由1增至2，合成provider仅收到剩余2条。
- 完成后数据库状态为embedding_set READY、12条向量、EMBEDDING task_step READY，parse task推进至READY/PUBLISH且task_version不变。第0批仍是SUCCEEDED/attempts=1，第1批变为SUCCEEDED/attempts=2。harness与临时DB记录已清理；未新增迁移、未运行自动化测试。

### 面试问题与边界

- **为什么批次调用账本要独立于run_epoch？** run_epoch是当前Worker的写入资格，批次账本则记录外部调用已经发生过几次以及哪些结果可以复用。新Worker接管时改变epoch，但不能把成功批次的请求预算清零。
- **成功批次为什么可以跳过？** 只有在输入/config指纹一致且已持久化向量行完整校验通过后，才能将SUCCEEDED作为可复用事实；本轮provider只处理剩余2条。
- **这能保证外部调用恰好一次吗？** 不能。若进程在供应商完成请求后、成功结果与账本提交前崩溃，接管者无法证明远端结果，可能再次调用并产生重复费用。持久化attempt上限将重复限制在配置预算内，但不提供exactly-once外部语义。
- **证据证明什么？** 实际Go处理器与本地PostgreSQL验证了成功批次复用、IN_FLIGHT批次受预算限制重试和产物完成；向量与provider响应均为合成数据，不证明百炼协议兼容、模型质量/费用或高并发竞争行为。


## 2026-11-11：M30 缩小Worker阶段事务锁范围

### Review发现与实现

`lockStageTask`原先对每个阶段提交依次锁user、video、parse task。user行锁并不参与阶段结果正确性或用户配额变更，却会让同一用户在不同视频上的PROBE、ASR、TRANSCRIPT、CHUNK、EMBEDDING和PUBLISH数据库事务互相等待，形成不必要的跨视频串行化。

现在Worker阶段事务只锁属于该owner且处于ACTIVE生命周期的视频行，然后锁对应parse task并校验stage、lease owner、run_epoch、lease与deadline。这样同一视频的阶段写入仍与发布和删除互斥；同一用户不同视频的Worker提交不再因共享user行锁而串行。用户级上传/任务配额入口仍按既有设计锁user行。无Schema变化。

### 静态锁序核对与边界

发布与删除均锁user（删除流程）后锁video；Worker锁video后只继续锁自己的parse task，不会反过来请求user锁，因此等待路径单向：若删除先持有user并等待video，Worker可完成并释放video；删除随后重新读取生命周期并阻止新stage。API创建/取消/重试路径仍按user→video→task锁序。构建通过；未运行自动化测试或并发数据库实验，因此锁等待时间、真实并发吞吐与竞态仍待运行验证。

### 面试问题

- **为什么需要锁视频行？** 它是单个视频的发布指针和删除生命周期协调点。PUBLISH读active版本并比较`task_version`时持有视频行锁，确保两个版本完成顺序不同也不能让active指针倒退；删除也持同一行，阻止删除与发布交叉提交。
- **为什么移除user行锁不会破坏配额？** Worker阶段事务不创建任务或上传会话，也不增减任何配额预留；配额检查与预留继续在创建/完成入口的user锁事务中完成。阶段提交只触碰既有task与视频产物。
- **代价和证据边界是什么？** 以每个视频仍串行化换取一致性，但不同视频可并行。当前只有代码锁序review与Go构建证据，没有数据库并发运行或性能测量。


## 2026-11-11：M31 本地任务纵向演示

### 实现

- 扩展`LOCAL_DEMO_MODE`：`seed-demo`在固定版本1字幕结果上创建版本2任务，从持久化TRANSCRIPT checkpoint进入`WAIT_DISPATCH/CHUNK`，事务内登记Outbox事件和Inbox所需事件身份。重复运行会核对任务配置、placeholder source、TRANSCRIPT checkpoint和Outbox事件身份，不重置执行进度。源blob标为placeholder，不含可播放媒体字节。
- background在该模式仍使用生产Outbox relay、Kafka consumer、任务Manager和Worker；实际调用`ChunkProcessor`、`EmbeddingProcessor`、`PublishProcessor`。新增`EmbeddingIdentity`把profile/provider/region/workspace/model/dimension/batch参数纳入config hash；云端默认构造器仍固定百炼身份，本地demo使用`local_demo_embedding_v1/local-demo/deterministic-unit-vector-v1`，避免合成向量被记成百炼产物。
- 本地provider对字幕关键词返回确定性1024维单位向量。API使用既有固定QA provider；任务完成后可从新发布版本走pgvector检索和完整问答SSE。没有更改生产OSS/百炼选择，没有新增迁移。

### 本地运行记录

- 本地PostgreSQL/Kafka环境此前没有demo账户或视频；`cmd/seed-demo`新建固定视频、版本1结果、版本2任务和Outbox事件。background日志实际显示同一task_version=2依次领取CHUNK/epoch 1、EMBEDDING/epoch 2、PUBLISH/epoch 3。
- 数据库状态为task SUCCEEDED/PUBLISH，TRANSCRIPT/CHUNK/EMBEDDING/PUBLISH四个step均READY，Outbox SENT一条、Inbox接收一条；版本2 TEXT结果成为active_result_id。Embedding集合记录为`local_demo_embedding_v1`、provider=`local-demo`、model=`deterministic-unit-vector-v1`，1024维向量已落pgvector。
- 以本地API运行登录和创建conversation均返回201；提问HTTP 200，SSE出现accepted、progress、answer、done，答案状态answered。
- `gofmt`、`go build ./...`和`git diff --check`通过；重复运行`seed-demo`成功复用完成后的task_version 2且不重置进度；未运行自动化测试。API/background保留本地运行进程，演示账号及记录仅用于本机开发。

### 面试问题与边界

- **为什么provider身份要进入Embedding配置指纹？** 同一文本用不同模型/profile生成的向量不可混用；持久化身份使恢复和检索集合能明确区分真实百炼模型与本地替身。
- **本地链路真实覆盖了什么？** Outbox、Kafka、Inbox、任务epoch、生产CHUNK/Embedding/PUBLISH仓储、版本发布、pgvector检索、Agent编排、答案校验和SSE均走了本地运行代码。
- **它没有覆盖什么？** 任务从合成字幕开始，跳过真实上传、PROBE/AUDIO、FFmpeg、Fun-ASR、OSS对象读取和Qwen；不能据此声称媒体正确、模型效果或云协议兼容。正式调用仍需OSS/百炼凭据与媒体工具。


### M32：本地内容Worker与来源引用

新增`NewLocalDemoService`和`NewLocalDemoSnapshot`，显式使用`local-demo/local/local-demo/deterministic-content-v1/local_demo_content_v1`身份；本地API与background都会启动内容服务/Worker。内容Worker领取时继续按profile的workspace隔离，处理器还会校验provider、region、workspace、model和profile。生产`NewService`、`content_v1`以及百炼Qwen路径保持原样，无新增迁移。

本地结构化provider从请求中读取来源编号，只回显第一条字幕片段为summary/key point/chapter，并给出对应source_id。其目的仅是让实际的批次计划、调用预算、JSON校验、来源约束、revision发布和资源查询链路能够离线执行，不模拟语义生成。

运行证据：`gofmt`与`go build ./...`通过，未跑自动化测试。background真实领取并完成本地PG中的revision 2内容job：状态SUCCEEDED/DONE、epoch 1、1个READY批次、总模型尝试2次（提取1次＋合并1次）、artifact 1个、来源引用1个、章节1个。随后通过内容创建API受理revision 3（HTTP 202）并由background完成；同一幂等键重放HTTP 200并返回相同job，内容读取API HTTP 200返回发布artifact。临时API session直接插入本地auth_sessions以验证受保护内容路由和CSRF，验证后删除；没有重新验证login/password。

运行中还发现一条旧本地job将provider/model记成Qwen、workspace却是local-demo。新增身份校验拒绝并保留该任务为FAILED，没有改写旧profile或自动复用其结果；创建正确local-demo profile的新revision后成功。这说明配置身份检查在本地实际生效，但没有验证真实百炼调用或内容语义质量。

面试回答：测试替身必须有独立provider/model/profile，身份进入不可变快照与输入哈希，避免mock输出被误认为真实供应商产物。失败的旧配置应保留为可审计历史；新配置创建新revision，不回填修改旧任务。确定性provider只复制原文，因此本轮证明的是后台任务与证据引用机制可运行，不证明摘要质量。


### M33：完整校验内容profile快照

Review发现内容任务只验证模型身份的部分字段，提示词版本、批次限制、输入字节上限和调用预算等其余快照字段没有与当前执行器逐项比对；这些规则实际取自代码常量，未来更改后恢复旧任务可能套用新配置。

`Service.validateProfile`现在严格解码快照、构造当前执行器的预期快照，并要求完整struct相等、规范JSON的SHA-256与数据库`profile_hash`一致、预期profile hash相同。未知JSON字段也会拒绝。生产仍只接受确定的Qwen `content_v1`快照，本地模式只接受独立`local_demo_content_v1`；无迁移。

验证：`gofmt`与`go build ./...`通过，未运行自动化测试。重启M33 API/background后，通过受保护内容任务API创建revision 4（HTTP 202）；Worker以run_epoch 1领取并成功完成，证明完整local-demo快照被接受并发布。没有对profile内容或hash做故障注入，也没有真实百炼任务。当前实现仅支持一个profile版本；切换配置后未知旧profile会失败，若未来需要滚动升级中断恢复，应保留profile版本对应的处理器，而不能改写快照。

面试回答：执行身份不止模型名，还包括prompt、温度、输入预算、重试预算和输出边界；它们共同影响语义与费用。将全量配置做不可变快照及hash，恢复时校验可以防止无声配置漂移。扩展新profile时应创建新身份与revision，旧job由兼容旧版本的worker执行或明确失败，不能假装按新配置重放旧产物。


## 2026-11-11：M34 本地媒体到解析结果纵向链路

### 实现

- 为媒体、字幕、ASR处理器抽出ArtifactStore窄接口。OSS客户端继续实现生产存储；新增local-demo文件存储保存对象版本、大小、SHA-256和metadata，提供固定版本HEAD、受限下载及不可变写入。
- ASR处理器改用provider接口，生产构造保持百炼Fun-ASR路径；本地构造使用local_demo_asr_v1、local-demo provider与deterministic-asr-v1模型标记。新增迁移000021和local_demo_asr独立gate。
- 新增seed-media-demo，幂等写入合成MP4并建立task_version 1/PROBE/Outbox；新增FFmpeg/FFprobe容器wrapper、媒体工具Containerfile和可重建素材脚本。
- 上传API与生产OSS选择没有改变；本地ASR为固定结果适配器，未部署Whisper或调用任何云API。

### 实际运行

- 复用本机已缓存的容器镜像，以FFmpeg 5.1.9/FFprobe 5.1.9处理本地合成视频；新增Containerfile的构建步骤尚未执行验证。database.Migrate应用000021；实际Outbox relay、Kafka consumer和Inbox把任务置为READY。
- background依次领取PROBE epoch1、AUDIO epoch2、ASR epoch3并持久等待；重新领取ASR epoch4后恢复并提交合成原始结果；TRANSCRIPT epoch5、CHUNK epoch6、EMBEDDING epoch7、PUBLISH epoch8完成。
- PostgreSQL最终查询：解析task version1为SUCCEEDED/PUBLISH，7个阶段step均READY，video file_state=VALID、时长7292ms，active_result_id非空。local-demo对象包含SOURCE_VIDEO 94364字节、AUDIO 206644字节、ASR_RAW 230字节；ASR submission只产生1次，身份local/local-demo/deterministic-asr-v1，状态SUCCEEDED。结果有2个转写句、1个chunk和1个1024维本地向量。
- 再次执行seed-media-demo返回相同video_id/task_id，保持完成状态与epoch不变。gofmt、go build ./...和git diff --check通过；未运行自动化测试。

### Review与限制

- local filesystem store只覆盖ArtifactStore接口，不承接multipart上传、OSS签名真实性、云版本列表/删除或生产生命周期。
- 当前合成TTS素材与固定ASR字幕语义相匹配，但ASR适配器不读取音频内容。结果只用于验证耐久流水线，不是识别质量数据。
- 生产ASR profile hash保持原格式，确保接口重构不造成旧任务的submission ledger无法恢复；本地profile独立包含provider/workspace身份。
- 真实用户文件上传仍要求OSS凭据；Fun-ASR、百炼Embedding和Qwen仍未调用。

### 面试问题

- **为什么本地适配器实现窄接口？** 防止业务处理器绑定云SDK，并让本地验证只替代当前必要的对象能力；生产上传合同仍由OSS全接口负责。
- **如何防止测试替身污染生产结果？** 通过profile/model/provider/region/workspace身份、不同profile hash及独立并发gate，形成可审计边界。
- **这条链路的恢复点在哪里？** 每个stage写task_steps，媒体源与音频写不可变blob，ASR提交单独有submit ledger。等待ASR时持久化下次执行时间并释放本地slot，重新领取使用新run_epoch，但继续同一task_version。


## 2026-11-11：M35 AUDIO对象写入后的检查点恢复

Review发现M34的AUDIO对象key使用随机UUID；如果OSS写成功而`CompleteAudio`事务未提交，重领任务没有数据库blob可查找，无法复用WAV。现在key由video ID、音频输入指纹、profile hash和task ID确定。AUDIO先找数据库READY产物，未命中时HEAD当前task的确定性key；找到则按固定版本下载，检查metadata、大小、SHA-256及WAV规格，再在当前run_epoch围栏事务中登记blob与task_step。超时和网络错误可重试，身份/内容损坏则显式失败，不覆盖不可变对象。M34旧随机key已经登记的音频仍能从数据库复用。

验证：gofmt、`go build ./...`及`git diff --check`通过；未运行自动化测试。本轮未注入对象写后DB提交失败，也未访问真实OSS；需在隔离任务中补做对象存在但无blob记录的运行验证。损坏对象无法覆盖，需新task_version重新解析并由对象GC回收无引用对象。


## 2026-11-11：M36 ASR迟到受理与被替代提交对账

Review发现：允许一次补交后，同一task可能有两条Fun-ASR任务；主Worker仅读取最新`submit_seq`，旧提交若在补交后迟到返回`task_id`，不会再被轮询，可能长期占用provider并发gate。任务终态后迟到的task_id也有同类问题。随后review发现不能把仍可人工继续的FAILED task视为终态孤儿，否则reconciler可能将最新ASR submission推进为`REMOTE_TERMINAL`，破坏人工继续恢复。

UNKNOWN等待现在使用数据库已保存的`next_poll_at`。ASR reconciler除了取消/删除场景，也会接管被更高提交序号替代的旧远端任务，以及任务已SUCCEEDED/CANCELLED或已不可人工继续FAILED后仍活动的远端记录；仍可人工继续的FAILED任务保留最新submission；按供应商状态查询至终态后标记`REMOTE_TERMINAL`。当前最新提交仍由解析Worker负责取回和保存结果。无task_id的UNKNOWN五分钟后最多自动补交一次；若第二次仍未知则停止自动补交，不宣称远端未受理。

官方Fun-ASR HTTP API按`task_id`查询，提交响应才返回task_id；文档没有描述通过本地operation_id查询或幂等键，因此五分钟只是系统设置的缓冲窗口。[API文档](https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-http-api)。验证：`gofmt -d`无格式差异、`go build ./...`通过；生产claim SQL在本地PostgreSQL 17通过PREPARE/EXPLAIN。追加review后，仍可人工继续的FAILED task保留其最新submission，最终SQL再次PREPARE/EXPLAIN通过。未修改数据库内容、未运行自动化测试、未调用Fun-ASR。local-demo background已重启，但它不运行真实供应商查询。


## 2026-11-11：M37 终态任务上的ASR UNKNOWN人工核实

Review发现两次ASR提交都UNKNOWN会令parse task失败，但人工核实命令仅允许已取消任务或视频删除中的submission。若失败任务的视频保留，取得供应商终态证据后仍无法释放provider并发gate。现在允许对`SUCCEEDED`、`CANCELLED`及不可人工继续的`FAILED`任务上的UNKNOWN无task_id提交进行人工处置；仍可人工继续的失败任务和活动任务都拒绝。每条提交仍要求operator、provider terminal status、evidence reference、reason和显式确认，审计与`REMOTE_TERMINAL`转换同事务提交，多条UNKNOWN逐条核实。

验证：`gofmt -d`无格式差异、`go build ./...`通过；未修改数据库、未运行自动化测试、未调用真实Fun-ASR或执行手工终态操作。运行中的本地background加载了当前代码，但LOCAL_DEMO_MODE不启用真实供应商处置。


## 2026-11-11：M38 人工继续与视频删除的锁竞争复核

复核`RetryParseTask`与`DeleteVideo`的事务边界。人工继续先锁owner用户行，再锁视频行，最后锁解析任务；锁内重新读取视频生命周期，并只允许`ACTIVE`、文件有效且源blob仍为READY的任务转为`READY`。视频删除采用相同的用户→视频锁顺序，在一个事务中将视频置为`DELETING`、取消活动解析任务并递增`run_epoch`。因此两个API事务按owner行串行：人工继续先提交时，后续删除会取消新任务；删除先提交时，人工继续看到DELETING/CANCELLED并拒绝重放。

解析Worker阶段提交按视频→任务锁定，不反向申请用户锁；删除若等待正在提交的阶段，只会等待Worker释放视频行，不形成循环等待。已有旧Worker也会因删除递增epoch/清除租约而失去提交资格。取消单个解析任务复用用户→视频→任务的顺序。

验证：逐段检查`resources.go`与`video_deletion.go`的锁SQL和状态条件，`go build ./...`通过。未运行并发数据库实验或自动化测试；本结论是基于代码路径的静态复核，不作为并发压测或真实竞态注入证据。未修改代码或数据库。

面试问题：**人工重试恰好和删除同时发生怎么办？** 两者先竞争同一个owner行锁，再以固定顺序锁视频和任务。事务提交顺序决定结果：重试先完成后删除会取消任务；删除先完成后重试会因生命周期/任务终态检查失败。`run_epoch`再阻止删除前已领取的旧Worker提交；锁负责串行化业务状态变更，epoch负责围栏异步执行者，两者解决不同层次的问题。


## 2026-11-11：M39 删除与结果/内容发布、问答受理的竞争复核

继续检查视频删除对并发写入的生命周期闸门。解析结果PUBLISH先锁ACTIVE视频，再锁解析任务；首个内容job、manifest、active result指针和任务完成检查点都在同一事务提交。删除按owner→video→parse task顺序，视频行互斥保证发布和删除只有一个先完成：发布先提交后删除会清空指针并进入回收；删除先提交后PUBLISH无法锁到ACTIVE视频。

内容revision受理和内容artifact发布都先锁视频再锁content job；删除先锁视频，再更新对应content job，顺序一致。问答受理与执行完成先锁owner，再锁视频，再锁conversation/question；删除也先锁owner和视频后中断问题、归档对话。问答开始时把`result_id`固定到question，删除前已完成答案不会重定向到另一解析版本；视频进入DELETING后不能新增问题，也不能提交旧执行的答案。worker租约/run_epoch与删除对任务的取消共同阻止旧执行者提交。

验证：静态检查`publish.go`、`content.go`、`questions.go`、`stages.go`与`video_deletion.go`中的锁顺序、生命周期谓词和事务提交边界；`go build ./...`通过。未运行并发数据库实验或SSE断连演练，未修改业务代码/数据库；本记录不代表真实并发负载验收。

面试问题：**删除和生成/问答同时发生，怎样保证不会复活数据？** 生命周期行锁给同一视频的状态变更提供串行化边界；业务事务按一致顺序锁video及其任务行。删除先提交时，后续写入因生命周期不是ACTIVE而失败；写入先提交时，删除随后冻结视频并清理该提交结果。`result_id`固定问题所读版本，lease/epoch则隔离已失租的异步执行者。


## 2026-11-11：M40 当前本地纵向链路状态复核

本机API的`/readyz`（8080与8081）均返回ready，background的9093指标端点可访问；PostgreSQL记录迁移至`000021_local_demo_asr_gate.sql`。以`LOCAL_DEMO_MODE=true`和本地PostgreSQL/Kafka配置再次运行`cmd/seed-media-demo`，命令成功并返回既有video/task ID。只读查询确认视频仍为ACTIVE/VALID且有active result；task version 1为SUCCEEDED/PUBLISH、run_epoch 8，PROBE/AUDIO/ASR/TRANSCRIPT/CHUNK/EMBEDDING/PUBLISH七个task_step均为READY。

这次运行验证了当前代码可连接本地依赖、演示种子对已完成任务保持幂等且不回退状态；它没有重新领取Worker任务，也没有模拟对象写入后DB提交失败。`go build ./...`通过。没有运行自动化测试；OSS_BUCKET与百炼密钥均未配置，未发起云调用。

同时复核M35调用链：AUDIO先查可复用READY blob；没有时按video/task/input/profile确定性key HEAD对象，固定版本下载后校验对象metadata、SHA-256、WAV codec/采样率/声道/时长，再由`CompleteAudio`在视频/任务锁和lease/run_epoch围栏下登记blob及阶段检查点。OSS适配器禁止覆盖，local-demo对象写入使用排他创建；删除扫描也覆盖`artifacts/audio/{video_id}/`前缀。未发现静态实现缺口，剩余证据缺口是隔离状态下注入“对象已写、数据库提交失败”并观察后续领取复用产物。

当前无`.env`文件，本轮直接以`.env.example`中的本地回环服务地址和本地演示数据库参数显式设置命令环境；既有本地API/background进程仍在运行，本轮未重启或改动其进程。后续要验证M35崩溃恢复，应在隔离视频/任务上构造可恢复对象窗口，不能改写当前已成功的演示结果。


## 2026-11-11：M41 AUDIO检查点故障恢复隔离运行

在PostgreSQL克隆库中创建task_version 3，只配置AUDIO阶段Worker，并对该task的AUDIO blob插入加一次性故障触发器。首次尝试成功将WAV写入本地对象存储；`CompleteAudio`在登记blob时被触发器拒绝，事务回滚。该次运行暴露`RecordStageFailure`的SQL参数类型问题：`jsonb_build_object`中的`$3/$4/$5`没有显式类型，PostgreSQL返回`could not determine data type of parameter $3`，导致失败无法入账。另发现ASR reconciliation和删除重试采用同样的未定型`jsonb_build_object('code',$N)`模式。

已将阶段失败参数显式转为text/text/boolean，并为ASR reconciliation、视频删除重试的错误码参数显式转为text。随后在隔离库的新task上重跑：日志显示epoch 1的注入错误被记录为`failure_count=1,retryable=true`；task进入重试后由epoch 2领取，先HEAD固定对象版本、校验SHA/WAV，再写入新的READY blob引用与AUDIO检查点，任务推进到ASR。最终DB状态为task `READY/ASR/run_epoch=2`、AUDIO step `READY/failure_count=1/last_run_epoch=2`；一次性序列表明注入点仅首次失败。对象键不可覆盖，第二次成功只能由现存对象恢复，未重新执行FFmpeg。

隔离数据库、临时Worker代码、故障触发器和本次生成的两份task对象已清理；原数据库仍为4个解析任务，原演示task保持`SUCCEEDED/PUBLISH/run_epoch=8`，API readiness仍为ready。`go build ./...`与`git diff --check`通过。未运行自动化测试、未调用OSS/Fun-ASR/百炼；验证使用本地文件对象存储和本地合成媒体。

面试问题：**怎样验证对象存储与数据库之间的崩溃窗口？** 在隔离环境让不可变WAV先成功写入，再让持久化blob/checkpoint事务失败；下一次领取用同一task_version但新run_epoch，按确定性键查找并固定版本下载，校验metadata、SHA与WAV规格后原子补写DB。故障注入还帮助发现失败处理器本身SQL类型不明确的问题，说明恢复验证需要观察失败状态也能可靠持久化，而不能只检查重试成功路径。


## 2026-11-11：M42 ASR旧提交对账与UNKNOWN人工核实隔离运行

在本机数据库克隆库上为一个已进入终态的解析任务构造已知Fun-ASR task_id的RUNNING submission，并构造一条无task_id的UNKNOWN submission。通过临时Go runner调用真实`ASRReconciliationRepository`：第一次claim后执行`RecordQueryFailure`，验证查询失败可以持久化；推进下一次轮询时间后再次claim，reconcile epoch递增到2，再记录供应商终态`SUCCEEDED`。最终已知远端作业变为`REMOTE_TERMINAL`，provider状态、两次尝试、epoch和终态时间均落库。该过程也运行覆盖M41新增显式类型转换的SQL路径。

随后两次执行正式`cmd/asr-unknown-resolve`命令，使用隔离库中的合成证据标识、明确的`NOT_ACCEPTED`终态、操作人和原因，并显式提供`I_VERIFIED_PROVIDER_TERMINAL`确认参数。第一次把UNKNOWN转为`REMOTE_TERMINAL`并写入人工审计；完全相同的第二次请求幂等返回，审计表仍只有一条记录。此验证覆盖命令参数、仓储事务和重复执行行为，不表示真实供应商已提供证据，也没有触发任何人工处置到生产数据。

克隆库、临时runner和dump已清理；原数据库仍有4个解析任务，未更改原任务。API readiness正常，`gofmt -d`无格式差异，`go build ./...`通过。没有运行自动化测试，也没有调用Fun-ASR、OSS或百炼。克隆库中的task_id、provider状态、evidence_ref和操作人均为合成夹具。

面试问题：**为什么要给ASR submission单独做reconciler和人工UNKNOWN处置？** POST超时可能让远端已受理、而本地没有task_id；有task_id但已经被补交替代或本地任务终止的作业，也不能再由主Worker安全负责。独立reconciler对已知task_id按自己的租约/epoch持续查到供应商终态；无task_id时供应商API没有可查询标识，只有在本地任务已不可继续后，才能凭明确的供应商终态证据由操作者逐条解除UNKNOWN，并在同一事务中留审计。自动对账与人工证据不能相互冒充。



## 2026-11-11：M43 终态内容任务失败告警

Review队列采集SQL与Prometheus规则时发现，`queueSnapshotSQL`会输出`content_job/FAILED`的持久状态，但现有16条规则没有消费该series；Outbox永久失败已有对应告警，内容任务永久失败却没有数据库状态告警。加入`MirrorCoreContentJobsFailed`，仅匹配background采集器的终态FAILED数量，大于0立即发warning，提示检查失败内容任务或创建新revision。

在本机Prometheus容器执行`promtool check rules`，确认17条规则语法有效；向Prometheus发送HUP加载配置后，查询API确认新规则`MirrorCoreContentJobsFailed`处于firing，`value=2`。本机background采集器实际报出两条旧的FAILED内容任务，进一步聚合DB仅检查了有限的profile/error字段：两条均为`CONTENT_PROFILE_INVALID`，profile为旧的`aliyun-bailian/content_v1`且workspace标记为`local-demo`，模型调用次数为0。这是此前本地演示留下的终态数据，未更改、重试或删除这些行；本次仅补监控规则并验证它能对持久终态发出告警。

`MirrorCoreQueueSnapshotFailed`保持0，Prometheus的API与background目标均为UP，队列指标查询成功。`gofmt -d`无格式差异、`go build ./...`通过；没有自动化测试，没有修改业务代码、数据库和远端服务。该验证覆盖一条真实本地失败状态的告警触发，不表示其他队列状态、数据库查询故障或Kafka积压告警都已演练。

面试问题：**为什么终态业务失败也要做持久指标，而不能只看Worker错误计数？** Worker错误计数只记录进程观测到的尝试，进程重启后Prometheus counter可能重置；终态任务保存在数据库里，问题可以跨进程重启持续存在。用低基数的`queue/state`聚合暴露数量，告警读取这个持久状态；错误明细留在业务表供授权排查，避免把用户文本、任务ID或任意错误内容放进Prometheus label。


## 2026-11-11：M44 ASR对账租约接管与epoch围栏隔离验证

在本机PostgreSQL克隆库中，将一个合成Fun-ASR submission关联到已完成解析任务，并通过临时Go runner调用正式`ASRReconciliationRepository`。owner A领取后，owner B立即claim得到空结果，说明未过期租约阻止并发重复领取。随后只在克隆库将A的lease_until推到过去，再由B接管；reconcile_epoch从1递增到2。

B接管后，分别用A持有的旧job调用`RecordProviderStatus`和`RecordQueryFailure`，两条写入均因owner/epoch/lease条件不匹配而返回lease lost。B持有当前epoch的终态提交成功，最终行是`REMOTE_TERMINAL`、epoch 2、owner为空且有远端终态时间。验证证实同一个submission可以安全接管，同时迟到的旧owner不能覆盖新owner结果。

临时克隆数据库、dump和runner已清理。原数据库仍为4个解析任务，原submission仍是`SUCCEEDED/epoch=0`。这是在单个Go runner中顺序驱动两个repository owner并人为推进过期时钟的数据库隔离验证，不是多进程压力或真实Fun-ASR网络竞态测试；本轮没有访问云服务、修改原库或运行自动化测试。

面试问题：**分布式租约过期后，旧worker迟到写回怎么办？** 只检查`lease_until`不够，因为旧worker可能在新worker接管后仍然返回。每次领取都递增单调epoch，并在结果写入时同时比较submission状态、epoch、owner和租约有效期。新worker接管会使旧epoch失效；旧worker的成功和失败路径都会被同一围栏拒绝，只有当前持有者能提交。


## 2026-11-11：M45 队列快照采集失败与恢复指标

用临时Go harness构造两个独立的`observability.Metrics`实例，分别连接不存在的隔离数据库和本机正常数据库，通过真实`Metrics.Handler()`抓取Prometheus文本。不存在的数据库使`queueSnapshotSQL`查询失败，采集器输出`mirrorcore_db_queue_scrape_error{service="background"} 1`；连接本机数据库时输出同一指标值0。结果验证了collector对查询失败和数据库恢复的指标行为。临时harness已删除，没有改动数据库或现有进程。

本机Prometheus规则文件已通过17条规则语法检查，`MirrorCoreQueueSnapshotFailed`表达式已加载；本轮没有把临时harness加入Prometheus targets，因此没有观察该规则进入firing。该验证也没有模拟查询中途断开或rows.Scan异常。API readiness保持正常，未运行自动化测试，未调用云服务。

面试问题：**数据库查询失败时，为什么Prometheus抓取仍要返回有效指标？** 队列SQL失败不应让整次`/metrics`变成不可抓取；否则就无法区分“服务不可达”和“服务可达但业务快照不可用”。collector保留`up=1`并报告`queue_scrape_error=1`，恢复后回到0；独立告警依据错误指标触发，数据库明细和日志用于定位。


## 2026-11-11：M46 Kafka consumer group lag监控

为落实监控设计表中的Kafka消费lag，新增`KafkaConsumerLagReader`与Prometheus collector。后台复用Kafka producer底层的franz-go client，经kadm读取指定parse consumer group的committed offsets，并读取目标topic每个partition的start/end offsets；lag按`max(0, end_offset - max(committed_offset, start_offset))`计算。group/topic仅采用代码中的固定消费组和topic作为指标标签；partition、group/topic、service构成有限维度。collector使用3秒上下文，导出每partition lag、committed offset是否早于保留start offset、采集耗时和`mirrorcore_kafka_lag_scrape_error`，不把Kafka错误文本放进labels。新增Kafka snapshot失败critical告警、总lag大于100条持续5分钟的warning告警，以及committed offset落在保留起点之前的critical告警；本地规则共20条。

本机Kafka 4.1.1临时创建一个单partition topic并写入5条合成记录。group没有提交offset时reader报lag=5；手工提交offset 3后报lag=2且offset未过期。随后提交offset 0并用DeleteRecords将log start推进到3；reader仍报告可读lag=2，同时`offset_behind_start=1`。Prometheus handler正确输出lag、越界指标和scrape error=0；再以实际parse group和生产topic读取，确认固定labels有效。临时topic和runner均清理。重启本机background到当前代码后，`/metrics`持续报告生产group partition 0 lag=0、offset_behind_start=0、scrape error=0；Prometheus API已抓取这些series，20条告警规则语法通过并已加载。本轮没有人为制造生产消费积压，也没有触发高lag/Kafka查询失败/offset过期告警。

`go build ./...`与`gofmt`检查通过；本机API readiness正常。后台进程使用`LOCAL_DEMO_MODE=true`，没有调用云服务；原数据库及4个解析任务未更改。运行证据来自单节点本地Kafka，不说明生产多broker或授权配置下的表现。

面试问题：**Kafka lag为什么比较end offset与committed offset？** consumer关闭自动提交，先将事件可靠写入Inbox，再提交offset，所以committed offset表示数据库已接收并确认到哪里。用end offset减committed offset估算Kafka里尚未完成handoff的记录；高lag告警反映交接消费者落后，数据库里的WAIT_DISPATCH任务年龄则覆盖Kafka之后的处理阻塞。无提交offset时按本项目的AtStart策略以partition start offset作基准；offset早于当前保留起点时按可读取起点计算，避免把已过期记录当成仍可消费积压。


## 2026-11-11：M47 Kafka拒收审计计数进入队列快照

对照监控设计中的拒收计数要求，发现`kafka_rejections`虽在PostgreSQL持久化并关联DLQ Outbox事件，但`queueSnapshotSQL`未暴露聚合指标。新增固定的`queue="kafka_rejection", state="REJECTED"`快照行，报告累计拒收条数和最老拒收记录年龄；不读取payload前缀和reason，也不把topic/partition/offset值放进Prometheus labels。该表保存的是审计记录，记录可能长期存在，因此它用于查看数量和年龄，不按累计非零自动告警。

重启本机background运行当前代码后，实际`/metrics`显示拒收数量与最老年龄均为0，数据库队列快照错误为0；Kafka lag仍为0、offset过期标志为0、Kafka snapshot错误为0。API readiness正常。`go build ./...`与gofmt检查通过；没有改数据库、运行自动化测试或调用云服务。当前库没有拒收样本，因此本轮验证了空集合下的聚合查询和指标导出；非零持久拒收记录的显示仍未实测。

面试问题：**为什么DLQ拒收记录不能用`count > 0`直接做告警？** 拒收表是审计记录，成功投递到DLQ后也会保留，非零只说明历史上发生过拒收，不代表当前交接积压。把它作为低基数的持久聚合供排查；当前DLQ Outbox仍处于PENDING/SENDING/FAILED时，由Outbox状态与年龄指标说明是否有待处理工作。


## 2026-11-11：M48 队列快照失败告警实触发

用临时Go metrics端点连接到不可用的隔离PostgreSQL端口，仅注册真实`queueCollector`并暴露`/metrics`。该端点继续返回HTTP成功，但`mirrorcore_db_queue_scrape_error{service="background"}`为1。启动独立、临时Prometheus 3.16.0实例，抓取间隔1秒，挂载项目原版`monitoring/alerts.yml`；target状态保持UP、快照错误series为1，`MirrorCoreQueueSnapshotFailed`先进入pending，在持续超过规则配置的60秒后进入firing，severity为critical。验证使用真实规则文件和Prometheus规则引擎，不改生产Prometheus配置，也不影响本地数据库连接。

隔离Prometheus、临时metrics harness及配置已清理。原background和API仍运行，正常background的queue scrape error恢复为0，Kafka lag/error为0，API readiness正常。未修改数据库、未调用云服务、未运行自动化测试。

面试问题：**怎样区分指标服务不可用和指标背后的数据库依赖失败？** `/metrics`目标仍为UP，但业务快照collector用独立的`queue_scrape_error=1`显式标记依赖失败；Prometheus对该指标设置持续时间告警。这样既保留可抓取性，也能将依赖故障和进程不可达分开定位。


## 2026-11-11：M49 Kafka lag与offset过期告警实触发

本机Kafka临时创建单partition topic，写入105条合成记录，把测试group committed offset设为0，再将partition保留起点推进至3。生产代码的`KafkaConsumerLagReader`读取到end offset=105、可读lag=102、`offset_behind_start=1`，采集错误为0。临时metrics端点的Prometheus target保持UP。

启动独立Prometheus 3.16.0，1秒抓取和评估一次，直接挂载项目`monitoring/alerts.yml`。`MirrorCoreKafkaOffsetExpired`先pending、持续1分钟后以critical进入firing；`MirrorCoreKafkaConsumerLagHigh`在lag持续大于100超过5分钟后以warning进入firing，观测值102。验证真实规则表达式、标签、for时长及告警严重级别均生效。

隔离Prometheus容器、临时metrics runner、配置和Kafka topic均已清理；本机原background、Prometheus、数据库与生产topic未改动。`go build ./...`、`gofmt -d`及`promtool check rules`通过（20条规则）；本机API readiness正常，background生产group lag、offset过期和采集错误均为0。未运行自动化测试，未调用云服务。实验是本机单节点Kafka合成流量，不覆盖多broker、生产授权、真实消费故障或高负载条件。

面试问题：**为什么lag告警和offset过期告警需要分开？** lag表示当前保留范围内消费者还欠多少可读记录，可用于检测持续消费落后；committed offset早于保留起点意味着部分记录已不可读取，是数据缺口信号。后者不能只靠一个很大的lag数字表示，所以用单独的布尔指标和critical告警；两者持续时间也分别根据噪声和恢复成本设定。


## 2026-11-11：M50 Outbox队列告警与拒收计数非空验证

通过pg_dump/pg_restore创建本机PostgreSQL隔离克隆，只在克隆库插入一条`next_send_at`已过期5分钟的PENDING Outbox、一条FAILED Outbox，以及引用FAILED Outbox的Kafka拒收审计记录。原始库没有这些topic/event key；克隆库通过真实`queueCollector`导出PENDING=1、FAILED=1、`kafka_rejection/REJECTED=1`，最老PENDING年龄约339秒，snapshot scrape error=0。

独立Prometheus 3.16.0以1秒抓取并加载项目真实20条告警规则。`MirrorCoreOutboxFailedEvents`以critical firing；`MirrorCoreOutboxStale`在状态持续超过2分钟后以warning firing。克隆原本包含的两条FAILED content job也触发`MirrorCoreContentJobsFailed`，其状态和计数与原库既有快照一致。拒收审计非零只导出计数与年龄，没有新增“count>0即告警”，避免把历史审计事实误当成待处理故障。

隔离Prometheus、runner、配置、dump及克隆数据库均已清理；原库对M50 topic/event key和拒收topic的计数为0。`go build ./...`、`gofmt -d`及`promtool check rules`通过（20条规则）；本机API readiness正常，background queue/Kafka采集错误与生产Kafka lag均为0。未运行自动化测试、未调用云服务。数据库克隆及单机Prometheus实验验证了SQL快照与告警表达式，不代表生产规模下队列聚合开销或消息投递可用性。

面试问题：**为什么队列监控要同时看状态数量和最老到期年龄？** 数量表示当前需要处理/人工处置的规模；年龄表示调度是否超出服务目标。历史拒收是审计记录，累计数非零并不一定代表仍有待处理故障，因此把它作为可查询聚合；Outbox的PENDING/SENDING与FAILED则按到期年龄和状态分别告警。


## 2026-11-11：M51 parse task、删除与ASR队列告警实触发

从本机PostgreSQL创建隔离克隆，在克隆中构造12分钟未处理的WAIT_DISPATCH parse task、租约已过期的RUNNING parse task、12分钟超期的PENDING video deletion job、provider deadline已过的ACCEPTED ASR submission，以及超过7分钟且没有provider task ID的UNKNOWN submission。仅在克隆库更新视频指针和生命周期，使测试任务与版本关系符合约束；真实API/background没有连接这个克隆。

真实`queueCollector`输出parse任务数、过期租约数、deletion队列年龄、ASR overdue deadline及unknown_no_task_id年龄；scrape error=0。独立Prometheus 3.16.0抓取真实20条告警规则，target保持UP。`MirrorCoreExpiredWorkLease`、`MirrorCoreASRDeadlineOverdue`、`MirrorCoreASRUnknownWithoutTaskID`在1分钟后进入firing；`MirrorCoreParseTaskQueueStale`和`MirrorCoreDeletionQueueStale`在5分钟后进入firing，严重级别分别为warning和critical。克隆中原有两条FAILED content job同时维持`MirrorCoreContentJobsFailed` firing。

隔离Prometheus、runner、配置、dump和克隆数据库已清理；原库无M51合成task/submission/deletion ID。未调用云服务，未运行自动化测试。本轮只证明单节点本地PostgreSQL数据能穿过生产collector和Prometheus规则引擎；不代表生产多实例、真实供应商deadline或删除对象成功率。

面试问题：**ASR deadline告警是否意味着可以把submission标成FAILED？** 不意味着。deadline是本地预设的远端处理时限，超时表示需要检查或对账；供应商任务可能仍在运行或状态未知，不能用本地计时器替代外部终态。尤其UNKNOWN且没有provider task ID时，告警要指向人工/供应商核实流程，而不是自动重试来猜测是否受理。


## 2026-11-11：M52 解析版本发布顺序与并发切换验证

在当前PostgreSQL隔离克隆中保留版本1真实已发布产物，为版本2至7建立具备有效租约的PUBLISH任务，并复用只读的READY转写、chunk、embedding集合。通过真实`StageRepository.PublishResult`执行发布，而非直接更新`active_result_id`。

latest_task_id已指向版本7时，版本2先完成仍返回`PUBLISHED`并把active推进至2，随后版本3推进至3；版本5先于版本4完成后，版本4返回`SKIPPED_OLDER`，仍正常SUCCEEDED且active保持5。随后并发调用版本6/7的发布事务，版本7返回`PUBLISHED`，版本6在该运行中返回`SKIPPED_OLDER`，最终active=7。task_step输出的outcome与active结果版本相符，证明视频行锁串行化并防止active版本回退，也覆盖用户确认的“较新任务仍构建时，中间已完成版本可先上线”。

隔离克隆、临时runner、dump已清理；原库未创建M52 task/result，API与background未受影响。`go build ./...`、`gofmt -d`与20条Prometheus规则检查通过；本机API readiness正常。未运行自动化测试或调用云服务。此验证使用已有本地合成媒体产物，不证明真实多实例生产负载下的延迟与死锁率。

面试问题：**latest_task_id指向版本7，为什么版本2还能发布？** latest表示最近受理的任务，不代表当前生效结果，也不作为发布门槛。发布事务锁视频并比较候选task_version与active result版本；2大于1即可上线。若版本5随后先于4生效，4完成时看到active=5，就保留自己的不可变结果并标记`SKIPPED_OLDER`，不回退线上指针。


## 2026-11-11：M53 Provider quota账本周期清理

Review发现旧实现按准入次数触发清理；低流量workspace的失效行会长期保留，清理SQL失败还会直接拒绝当次准入。现在清理已从准入关键路径移除，新增包级`RunLedgerPruner`：启动时立即运行，此后每分钟扫描所有workspace/model的过期账本。每轮最多10个500行批次，按`created_at,id`排序并使用`FOR UPDATE SKIP LOCKED`，允许API/background多实例协同。新增`000022_provider_quota_prune_index.sql`为全局过期扫描建立`(created_at,id)`索引。清理错误只通过回调写warning，不改变额度判断。

API与background各启动一个进程级pruner；API使用maintenance context和WaitGroup，background把它计入component生命周期，优雅停机时取消并等待退出。即使旧workspace/model已不在当前配置中，其账本也可回收。RPM/TPM门闩、滚动60秒窗口、保守预留、usage回填及429共享冷却保持不变。两分钟保留期大于当前Provider HTTP超时，避免清理仍在途的预留。

在隔离PostgreSQL克隆中，对没有活动Limiter的已移除workspace插入一条3分钟前账本，启动全局pruner后该行被清理。注入账本DELETE失败后，pruner回调报告错误，额度`Reserve`仍成功。另一次隔离运行验证两个独立pgxpool共享RPM=1时只有一个并发准入成功，以及共享gate配置不一致时拒绝启动。隔离克隆、临时runner和dump已清理。`go build ./...`、`gofmt -d`、`git diff --check`与Prometheus 20条规则语法检查通过。新API在独立端口启动并通过readiness，启动时清理了本机DB中的合成旧workspace账本，随后优雅关闭；现有API/background进程没有重启。云服务未调用。

面试问题：**配额账本清理为什么不放在准入关键路径？** 准入只依赖过去60秒的记录，旧行不影响RPM/TPM判断。清理属于存储维护，应独立、分批、可观察地运行；清理暂时失败时，仍按有效窗口做额度判断，避免维护故障扩大成模型请求拒绝故障。

## 2026-11-11：M54 quota账本清理失败告警

复核M53后发现全局清理错误只写日志，缺少可聚合的监控信号。新增`mirrorcore_provider_quota_prune_failures_total{service}`，由API与background的清理回调递增；只带固定服务标签，不记录workspace/model或错误文本。新增`MirrorCoreProviderQuotaPruneFailed`告警，5分钟内计数增加即进入warning状态，与provider调用后的reservation持久化失败指标分开表达。

在本机数据库对一个合成过期reservation注入DELETE触发器，新代码API以LOCAL_DEMO_MODE在独立端口启动。清理器触发错误后记录warning，抓取到`mirrorcore_provider_quota_prune_failures_total{service="api"} 1`，`/readyz`仍返回ready。对实际21条Prometheus规则执行`promtool check rules`通过，并用promtool规则引擎输入计数样本验证新告警按预期触发。

随后优雅关闭临时API，移除触发器、函数及合成账本；临时规则输入文件已删除，现有API/background未重启或被替换，云服务未调用。`go build ./...`、`gofmt`、`git diff --check`通过。M53/M54结果只覆盖清理路径与告警，不代表真实provider 429、额度等待或调用后账本写失败告警已触发。

面试问题：**为什么清理失败与provider调用后账本写失败要用不同指标？** 两种故障的操作语义不同：前者影响过期数据回收，短期内不影响额度准入；后者可能让真实调用的usage或失败状态没有落账。独立计数便于区分告警响应，同时共享数据库错误仍保留在日志上下文中。


## 2026-11-11：M55 Provider配额告警纵向验证

在M53/M54之外继续验证provider调用告警。隔离PostgreSQL克隆中使用生产`EmbeddingAdapter`和真实`Limiter`，provider端由本地fake实现。第一种响应模拟HTTP 429；adapter写入FAILED账本、设置共享冷却，指标输出`outcome="failure",error_class="rate_limited"`。随后在gate写入6秒冷却，成功调用等待约6秒后继续，直方图count=1、sum约6秒且le=5桶为0、le=10桶为1。再对成功reservation的SUCCEEDED更新注入数据库触发器失败，adapter返回账本持久化错误，`mirrorcore_provider_quota_persistence_failures_total{operation="complete"}`递增。

使用项目当前21条Prometheus规则运行`promtool test rules`，分别验证429和调用账本写失败告警立即firing；让高等待直方图样本持续跨越5分钟，验证`MirrorCoreProviderQuotaWaitHigh`的for条件和p95阈值。隔离数据库、触发器、临时fake adapter harness和规则输入文件均已清理。`go build ./...`、`gofmt`、`git diff --check`和21条规则语法检查通过；本轮未调用百炼。此结果证明adapter→数据库门闩/账本→metrics→Prometheus规则的本地代码路径，不证明真实供应商会按这些错误码或时延响应。

面试问题：**为什么429也要占一次RPM并写失败账本？** 供应商已经收到了一次HTTP尝试，失败仍消耗请求配额；账本保留结果、错误类别和request ID，429的Retry-After进入共享cooldown，避免API与background继续同时撞限流。实际计费和窗口仍以供应商账户规则为准。


## 2026-11-11：M56 API、DB pool与Kafka采集告警验证

使用本地隔离数据库与临时metrics harness驱动生产采集代码。`httpapi`真实router的`GET /readyz`连接不可达的PostgreSQL地址，返回503，HTTP中间件导出`mirrorcore_http_requests_total{route="/readyz",status="503"} 1`。另用真实pgxpool配置`MaxConns=10`并持有9个连接，collector导出acquired=9、max=10。Kafka部分通过真实`KafkaConsumerLagReader`连接本机未监听端口127.0.0.1:1，collector输出`mirrorcore_kafka_lag_scrape_error{service="background",group="mirrorcore-parse-admission-v1"} 1`。

用Prometheus规则引擎样本验证`MirrorCoreAPIHigh5xxRate`在5分钟高错误率后firing、`MirrorCoreDBPoolNearCapacity`在连接池90%持续5分钟后firing、`MirrorCoreKafkaLagSnapshotFailed`在快照错误持续1分钟后firing。21条规则语法与运行中的Prometheus规则加载均通过。临时数据库克隆、pgx连接、不可达Kafka client、规则输入文件和runner均已清理；未更改本机演示数据库或现有API/background进程，未调用云服务。规则的持续时间使用promtool时间序列模拟；此处证明本地指标和告警契约，不代表生产环境的真实错误率、连接池压力或Kafka故障。

面试问题：**为什么高错误率和依赖不可达不直接报警一个状态码？** 单次请求的503可能是短暂噪声，错误率告警按时间窗口看服务影响；DB pool告警观察资源饱和，Kafka快照错误告警观察监控能力本身是否失效。分开以后能区别业务故障、资源压力和观测链路故障。


## 2026-11-11：M60 问答轨迹导出与人工标注草稿

新增`cmd/qa-eval-export`：从JSONL标注计划读取成对question ID，仅访问PostgreSQL，不调用模型或云服务。导出前确认问题均为终态、question steps完整、策略与持久化generation profile一致、pair处于同一video与`result_id`、使用同一retrieval profile，并且初始排名及实际提供上下文均一致。策略配置ID由持久化generation/retrieval profile规范化后计算SHA-256；实验共有配置指纹仍由人工按冻结manifest提供。

扩展评估schema，记录question/result ID、原始answer JSON，并区分初始/最终检索排名与初始/最终真正交给模型的字幕。人工引用检查改为以实际provided证据为准，旧版JSONL缺少这些字段时保持原有兼容行为。新增`ValidateAnnotationDraft`只做草稿结构校验，不放宽正式`LoadJSONL`/`Evaluate`：成功的`human_annotated` run仍须人工审核及逐条claims标注。无完整账单依据时cost保持null；未记录的调用step不导出，避免将不完整轨迹按零计数。`qa-strategy-smoke`新增可选`-keep-questions`，默认路径依旧清理临时问题。

在本地PostgreSQL/合成媒体fixture执行fixed_rag与agent配对，均SUCCEEDED、固定同一result、初检排名和provided句序号均为[1,2]；Agent有一次无进展工具调用。用显式`synthetic_fixture`计划导出一case/两run，并成功交给`cmd/agent-eval`生成报告；将草稿标为`human_annotated`但不审核后，正式评估器按预期拒绝。两条临时问答记录已删除，查询数为0。`gofmt`与`go build ./...`通过；未运行`go test`或调用云服务。该执行仅验证数据库→轨迹草稿→离线评估器的接线，不提供人工质量、真实token/成本或Agent收益证据。

面试问题：**为什么同时保留初始排名和实际提供给模型的证据？** Top-K候选可能被证据字节预算裁掉；排名用于召回和MRR，引用有效性只能针对模型实际收到的字幕核验。否则系统会把模型没有见到的候选误判为合法引用。

面试问题：**如何避免评估草稿被误报成模型质量结果？** 导出器默认标记为未人工审核，claims为空；human数据的正式加载仍要求每条成功答案完成审核和声明级标注。合成fixture仅用于验证流程，其报告明确不代表模型/检索质量或生产成本。


## 2026-11-11：M57 Agent配对效果评估工具

新增`internal/agenteval`离线评估包和`cmd/agent-eval`入口，按D1在相同初始检索下配对fixed_rag与agent。JSONL校验覆盖schema版本和未知字段、实验及公共配置标识一致性、同策略配置稳定性、题型/支持状态一致性、每题恰好两种策略、相同初检排序、Agent保留初检前缀、video_key不能跨dev/holdout、人工引用必须在实际最终证据中、失败运行不能附答案等。synthetic_fixture被禁止声明human_reviewed，避免演示夹具产出容易误读的答案质量数字。

报告按strategy/split/category分层，输出completion、expected status、必需证据组覆盖/完整覆盖/MRR、人工复核质量指标、工具及无进展调用、HTTP尝试、token/成本、平均延迟与P95；配对结果含case_id，视频数与P95 sample_count/rank分别显示。缺少人工审阅、成本或usage时分母为0且value为null。实验/共有配置指纹只用于追踪并关联外部实验清单，评估器无法证明实际请求与指纹对应，必须保存请求日志和完整manifest。

加入四类题型的synthetic_fixture共4 cases/8 runs。运行`go run ./cmd/agent-eval -input assets/evaluation/synthetic_fixture.jsonl`得到4 cases、4 videos、8 runs、8个分层summary与4个逐题比较；输出声明不衡量ASR、检索、回答质量、模型成本或Agent收益。零延迟字段只是schema夹具值，不是性能数据。`gofmt`、`go build ./...`、CLI运行与`git diff --check`通过；没有运行`go test`，没有调用云服务。

真实人工数据仍未创建。下一步需依法取得5段视频、人工定义50个问题/必要证据和答案要点，按来源视频固定dev/holdout，冻结实验manifest，再使用相同初检进行百炼配对运行与盲审。小样本结果只报告失败、样本量和观察值，不据此做泛化或SFT/RL收益结论。


## 2026-11-11：M58 固定RAG与Agent运行时配对基线

Review M57发现评估器虽能校验fixed_rag/agent JSONL，但Go问答服务实际只实现Agent路径。新增`qa.NewFixedRAGEvaluationService`与local-demo对应构造器，仅内部评估路径能选择固定RAG；公开API默认仍用Agent。策略写入受理时不可变`generation_profile`（strategy、prompt_version与tools_enabled/max_tool_attempts），Run按持久化profile恢复选择，避免任务中断后重试时默默换策略。旧profile无strategy时向后兼容为Agent。

固定RAG和Agent共用query构造、embedding模型、受理时固定result/transcript/embedding_set、Top-K检索、证据窗口整理、问题证据登记、最终answer_v1结构与引用来源校验、数据库提交。固定RAG直接向同一AnswerGenerator提交限定初检证据的answer_v1请求，不调用决策模型或工具；输出失败时沿用同一有限校验修复预算。Agent保留原决策/工具/收尾流程。初始RETRIEVAL metadata新增原始排名及模型初始证据句序号；每次TOOL metadata新增实际登记的字幕句序号，供后续追踪评估。

新增`cmd/qa-strategy-smoke`，只允许`LOCAL_DEMO_MODE=true`，从已完成媒体fixture查找同一视频，顺序执行两个strategy服务、数据库检索/证据登记、确定性provider回答和答案持久化，确认初检一致，汇总工具与无进展调用。命令使用随机请求键创建临时问题，正常退出按question_id清理。它用于控制流接线检查，不衡量语义质量。

首次本地执行发现无conversation创建问题时，`context_question_ids`的nil UUID slice以SQL NULL写入，违反NOT NULL约束。将无会话初始值改为空数组，再运行通过：固定RAG与Agent均`SUCCEEDED/answered`，两者初检排序均为字幕句`[1,2]`；固定RAG工具调用0，Agent工具调用1且新增证据0（无进展）。报告标为`synthetic_fixture`，耗时字段命名为`elapsed_ms_local_fixture`并注明不代表provider或生产延迟。直接检查数据库确认临时question记录数为0；API readiness仍为200。`gofmt`、`go build ./...`及CLI运行通过；未运行自动化测试，未调用云服务。

runtime smoke证明两条Go控制流、共享初检、答案Schema/来源校验和question step记录在本地串通；确定性fixture不能说明检索/回答语义或Agent收益。尚无自动导出人工标注集的链路，真实百炼测量和50题人工数据仍待完成。


## 2026-11-11：M59 评估配对结果版本围栏

Review发现M58 smoke虽验证两组初检句序号相同，但两条问答可能在不同生效parse_result上运行。新增可选`RequiredResultID`评估受理前置条件：QuestionRepository在锁视频并读取active_result后核验ID，不匹配返回`ErrQuestionResultChanged`且不插入问答；幂等键命中也核对已有question.result_id。该字段不进入用户请求canonical hash，公开API传nil，保持原业务幂等语义且无需迁移。

`cmd/qa-strategy-smoke`先将随机无效result_id交给受理层，实际收到`ErrQuestionResultChanged`；随后fixed_rag和agent均要求同一active result。真实运行结果两条均固定`1d6a4c2c-92ca-4eb6-b7e4-ab2ad8f34413`，初检[1,2]，都SUCCEEDED；固定RAG工具0次，Agent1次无进展调用。临时问答正常删除。

`gofmt`、`go build ./...`及smoke命令通过；没有运行自动化测试、没有调用云服务。错误版本被拒和正常同版本配对已运行验证；active_result在两次受理之间真实并发变化的竞态尚未演练。外部真实模型评估继续依赖可合法使用的标注视频和云账号。


## 2026-11-11：M61 冻结计划驱动的配对问答执行器

新增`cmd/qa-eval-run`，从预先冻结的问题计划创建fixed_rag与agent两条问答。两侧受理时用`RequiredResultID`锁定同一视频结果，策略配置写入generation profile；执行完成后核对result ID、初始排序和模型实际收到的初始上下文，完全匹配才写出可直接给M60 exporter使用的计划行。问题文本、视频、目标result、split、预期标签与必需证据组在运行前读取并校验。

request key由run ID、实验/case、问题/标注快照和策略确定生成；已SUCCEEDED问题重跑时复用，终态FAILED/INTERRUPTED要求只变更失败侧的`fixed_rag_run_id`或`agent_run_id`，不在同一个键下重复生成。`-output`以0600权限创建且拒绝覆盖，每完成一个case即flush；进程重启后可用同一plan继续并复用成功记录。单次默认最多运行1个case，`-max-cases`上限50。LOCAL_DEMO_MODE只接受`synthetic_fixture`；真实模式必须显式传`-allow-provider-calls`，并通过既有数据库RPM/TPM limiter执行百炼Embedding/Qwen调用。公开问答API策略不变。

长运行沿用问答服务的租约约束：runner每10秒续45秒数据库租约，单题deadline限定执行context；续租失败即取消执行。收到`ErrQuestionExecutionLost`时不再尝试以旧execution ID写FAILED/INTERRUPTED，避免失去租约的旧执行者干扰接管后的状态。

本地PostgreSQL与合成媒体fixture完整运行qa-eval-run→qa-eval-export→agent-eval：fixed_rag与agent均成功，共用结果版本、初检排名和提供上下文[1,2]，报告为1 case/2 runs的synthetic_fixture。相同plan第二次执行复用原成功问题，输出一致；真实Provider模式未显式加`-allow-provider-calls`时在provider调用前明确拒绝。测试问答记录按question ID删除，数据库剩余0条。`gofmt`、`go build ./...`通过；未运行`go test`或云调用。上述合成报告只证明计划→受理/策略→持久化→导出→评估器接线，不代表模型质量、真实费用或生产时延。

Review边界：共有配置指纹仍由实验者关联冻结manifest，CLI不能证明该声明与实际部署一致；含未完成question step的记录由导出阶段拒绝，不伪装成零次调用。多case真实百炼运行、账单对账与人工盲审仍未执行。

面试问题：**稳定幂等键为什么同时覆盖问题计划和标注？** 单靠随机key无法在CLI中断后安全恢复；只用case_id又可能把修改过的题目或标注重用成旧结果。把run、case、共有配置、冻结输入与策略一起摘要后，同一执行重启能复用成功问答，冻结数据变化则产生新身份；每侧独立run ID允许只重跑失败侧。

面试问题：**为什么评估Runner还核对实际初始提供上下文？** Top-K排序一致不保证证据预算截断后一致。若Agent第一轮拿到更多或不同的初始证据，就不能把最终差异归因于工具补查；所以只有初始排名和provided句序号都相同，runner才输出配对计划。


## 2026-11-11：M62 保护人工标注草稿不被重导出覆盖

Review发现`cmd/qa-eval-export -output`使用`O_TRUNC`，同一路径重导出会清空已人工填写的审核标志和claims。改为`O_CREATE|O_EXCL|O_WRONLY`并保持0600文件权限；若路径已存在则失败，标准输出模式不变。同步更新评估说明和README。

`gofmt`与`go build ./...`通过；未运行自动化测试或云请求。未对真实人工文件做故障注入；保护依赖操作系统创建文件时的原子`O_EXCL`语义。

面试问题：**如何避免评估工具破坏人工标注成果？** 导出是只创建新版本，不覆盖已有路径；新草稿需要新文件名，人工标签与原始模型输出因此不会在一次重跑中被静默替换。


## 2026-11-11：M63 A/B匿名盲审包与审核回填

新增`cmd/qa-eval-review pack|apply`。审核包随机打乱题序与每题A/B，省去策略、IDs、split、调用计数、usage、费用和延迟；答案键独立保存在0700目录中的0600文件。提交模板让审核者填写reviewer_id、answer block级claim和匿名引用，导入时通过源draft/pack SHA-256、private key映射与严格范围验证回填原策略及transcript sentence seq。只接受一名审核者/次导入；无仲裁聚合器，也不承诺候选文本无法自我泄露策略。

扩展Sample和运行计划记录问题文本与评分点说明，Exporter从固定转写拉取必需证据原文/时间，便于审核者核查；Human计划在生成pack时必须具备完整说明。

本地合成fixture临时生成4题pack/template，观察审核文件不含strategy、question/result ID、split或运行元数据，directory=0700、文件=0600；同名输出被拒且已生成内容哈希不变。apply拒绝synthetic_fixture。`agent-eval`原有4-case/8-run fixture命令仍运行通过，`gofmt`与`go build ./...`通过；未运行自动化测试、未接真实供应商，也没有真实人工submission。因此不能声称成功human apply路径已被运行验证，更不能声称模型质量已评估。


## 2026-11-11：M64 API关闭时取消在途问答

Review发现API进程原先只调用`http.Server.Shutdown`，未主动取消正在执行的问答。实现`httpapi.Runtime`生命周期句柄，保持既有Gin构造函数兼容；API主进程在HTTP shutdown前停止接收新的问答并取消已登记的execution context。新执行先登记取消句柄再发送`accepted`；受理与关闭共享同一互斥锁，关闭竞态中漏过注册的请求会被标为`INTERRUPTED`。

问答step导出器已经检查`question_steps.completed_at`，任何未完成step都会拒绝导出，避免把不确定的远端尝试记成零调用。编译后启动当前源码API，先观察空闲SIGINT关闭日志。随后在隔离LOCAL_DEMO API运行中临时加一个只匹配本地演练问题的PostgreSQL触发器，让初始RETRIEVAL step睡眠；收到首个progress后向API发SIGTERM。SSE顺序为accepted/progress/error，数据库最终为`INTERRUPTED`、`answer_json IS NULL`、错误码`EXECUTION_CANCELLED`；应用进程退出码0并记录`api shutdown complete`。演练用触发器/函数已删除，未留下schema对象。

随后用另一条只匹配演练问题的临时触发器将RETRIEVAL step延迟10秒。收到`accepted/progress`后客户端以TCP RST断开，数据库观察到`INTERRUPTED`、无答案、错误码`EXECUTION_CANCELLED`；会话归档、session撤销、触发器/函数删除均完成。`gofmt -d`与`go build ./...`通过；未运行自动化测试。上述故障注入验证了API关服和客户端断连均能取消活动问答并保护未提交答案，但未覆盖取消恰与成功答案提交并发时的边界，也不证明真实百炼HTTP客户端取消响应或15秒期限下的所有超时组合。

面试问题：**HTTP优雅关闭为什么还要主动取消长问答？** `http.Server.Shutdown`停止接收新连接并等待活动请求，不会替应用发出取消信号。先将问答入口标记为停止、取消已登记的执行context，再等待handler和数据库终态收敛，能减少进程退出时长；执行ID和租约条件仍负责阻止过期执行者提交答案。


## 2026-11-11：M65 重跑本地媒体与Agent问答纵向链路

使用`LOCAL_DEMO_MODE=true`运行`cmd/seed-media-demo`，稳定fixture创建/核验成功；本机现有background worker将task推进到`SUCCEEDED/PUBLISH`，`run_epoch=8`，PROBE、AUDIO、ASR、TRANSCRIPT、CHUNK、EMBEDDING、PUBLISH七个检查点均为READY。视频为ACTIVE/VALID，active结果为TEXT，task_version=1。所有媒体、ASR与Embedding均为合成/本地实现，没有外部云请求。

在不触碰8080/8081常驻API的情况下，用当前源码在`127.0.0.1:18080`启动LOCAL_DEMO_MODE API，`/readyz`返回200。使用本地演示会话经真实HTTP路由创建conversation，提交问题后收到accepted、progress、answer、done事件；通过GET读回question为SUCCEEDED、答案已持久化且固定到同一result_id。随后归档该合成conversation并撤销session。question_id=`0882d02e-7135-4c43-91e0-9a76bd05d494`。`/tmp/mirrorcore-m64-media`为本机替身对象目录。

向当前源码API发送SIGINT后，应用日志记录`api shutdown complete`，临时监听端口已释放；该轮关服时没有活动问答。随后M64补验以定向PostgreSQL延迟分别验证SIGTERM与客户端TCP RST：两次均持久化为`INTERRUPTED`且无答案。`gofmt -d`和`go build ./...`通过，本轮未运行自动化测试；localdemo固定答案只证明检索/工具/校验/SSE/持久化接线，不提供模型质量或百炼兼容证据。

面试问题：**如何区分本地纵向演示和真实供应商验收？** 本地纵向演示检查数据库状态迁移、固定版本、Agent调用、SSE与持久化是否连通；provider/model质量需真实凭据、真实输入及独立人工标注衡量。确定性替身不具备语义能力，不能以它报告正确率或云兼容性。


## 2026-11-11：M66 取消与成功答案提交并发竞态

修正前一版演练脚本：问答请求现在在读取SSE时立即解析`accepted`事件并取得question ID，不再等到整个响应结束后才发送取消。用当前源码API在隔离端口`127.0.0.1:18080`、LOCAL_DEMO_MODE及合成媒体上运行；不调用OSS或百炼。

临时PostgreSQL触发器只匹配唯一演练问题，并在`questions.status`转为`SUCCEEDED`时暂停提交事务4秒。数据库观察到该UPDATE处于`PgSleep`后，向真实HTTP取消接口发请求；同时观察到取消事务的`SELECT users ... FOR UPDATE`因owner行锁等待。延迟结束后，成功事务先提交，取消接口返回`execution_status=SUCCEEDED`；SSE发送`done/SUCCEEDED`，数据库最终为`SUCCEEDED`且`answer_json`非空。question_id=`206a2561-8019-4ead-8b48-f8c819950a1a`。演练conversation已归档、session已撤销；独立查询确认临时trigger/function数量均为0。新API日志记录`api shutdown complete`，8080/8081常驻API未触碰。

一次12秒暂停试验得到`INTERRUPTED`：成功事务持有question行锁期间，10秒续租ticker启动的`RenewQuestionLease`更新也被阻塞，其2秒调用超时按设计取消执行context。它没有证明取消与提交的胜负；将暂停缩短到4秒后，在不触发租约续期的窗口内完成目标竞态。这个结果同时说明测试注入需要避开租约机制本身，否则会混入另一条合法取消路径。

没有修改生产代码；未运行`go test`。该演练只验证本地Go/API/PostgreSQL锁顺序和最终状态，不代表真实百炼取消行为、真实网络延迟或生产并发压测。

面试问题：**答案提交和用户取消同时发生，谁赢？** 两条事务按用户→视频→问题的相同锁顺序串行化。若答案提交先持锁并完成条件更新，取消等待后会读到`SUCCEEDED`，不能改成`INTERRUPTED`；若取消先提交，答案事务重新检查执行状态时会因状态不是`RUNNING`而丢弃答案。HTTP取消接口与执行context取消分别处理持久化状态和进程内调用，不能用“调用了cancel函数”代替数据库状态裁决。


## 2026-11-11：M67 当前源码媒体与内容 Worker 修复及 API 纵向复核

为避免覆盖已完成的合成fixture，给`seed-media-demo`增加`-fixture-key`，并新增`SeedMediaFixtureWithKey`。默认`media-v1`保持原有稳定video/task/object身份；不同键派生独立video、task与对象键，同一键可幂等复用且不回退任务进度。

使用独立夹具键在当前源码启动的LOCAL_DEMO background上运行，首次从PUBLISH自动创建content revision时发现配置错误：`workspace_id=local-demo`的初始content job仍被固定为百炼profile，LocalDemo Worker因此以`CONTENT_PROFILE_INVALID`拒绝。修正`seedInitialContentJob`按workspace选择profile：local-demo使用`local_demo_content_v1`，正式workspace仍使用百炼profile。第一次失败任务保留作故障记录；新键重跑后媒体task到达`SUCCEEDED/PUBLISH`，run_epoch=8，七个阶段检查点READY；视频为ACTIVE/VALID，content job以`local-demo/local_demo_content_v1`完成revision 1并生成一个artifact。重复运行同键seed命令返回相同ID，没有重置成功进度。

随后在当前源码LOCAL_DEMO API (`127.0.0.1:18080`)经HTTP创建会话和conversation，问答SSE收到accepted/progress/answer/done；GET问答确认`execution_status=SUCCEEDED`、答案持久化且result_id仍固定在该active结果。conversation已归档，会话已撤销。此验证走本地确定性provider，不调用OSS、Fun-ASR、Embedding或Qwen；只证明Go服务和本地存储/数据库控制流接线，不证明供应商协议或语义质量。

本轮`gofmt`与`go build ./...`通过，未运行`go test`。面试问题：**同一条媒体流程为什么要有夹具身份？** 稳定键让重试可幂等复用，独立键让新演练拥有独立video/task，避免手工删除旧数据或把旧终态误当成新运行；键属于本地演示身份，不替代正式业务task_version和run_epoch。面试问题：**为什么本地演示内容任务也必须保存正确的profile快照？** Worker恢复执行要依赖受理时持久化的模型/处理配置，不能根据运行进程猜测；local-demo与云服务profile不同，选择错误必须在受理处纠正，否则新建任务会系统性失败。

当前源码API和background分别监听18080与19093并处于LOCAL_DEMO；既有8080/8081服务未触碰。真实OSS/百炼凭据仍未配置，供应商联调、人工数据质量评估及生产压力验证未完成。


## 2026-11-11：M68 内容任务profile选择回归保护

M67本地纵向运行发现PUBLISH自动创建的首个内容job会为local-demo误存百炼profile。把选择规则收敛到`contentprofile.NewSnapshotForWorkspace`：`local-demo`使用独立deterministic provider/model/profile，普通workspace仍使用既定百炼快照。新增单元测试验证两个provider身份、workspace/model/profile、`enable_thinking=false`、序列化hash及超长workspace拒绝。

另新增PostgreSQL集成测试，使用事务内的最小TEXT结果依赖行实际调用`seedInitialContentJob`，检查本地和正式workspace的profile/hash被正确持久化，并对相同结果重复seed后断言revision 1仍恰好一条；事务回滚清理夹具。测试不能替代完整PUBLISH worker或content worker端到端运行，M67已有该运行证据。

`TEST_DATABASE_URL=... go test -count=1 ./...`通过；这次启用了本地PostgreSQL测试，覆盖资源幂等/配额、Outbox租约/退避、HTTP session/CSRF以及本次新增profile持久化用例。Kafka/PostgreSQL集成测试因未设置`TEST_KAFKA_BROKERS`而跳过，避免加入运行中的本地consumer group。`go build ./...`通过；未调用云服务。

面试问题：**为什么profile快照和profile hash要一起持久化？** Worker接管后必须重放同一配置，不能因为部署环境变化而切换供应商或参数；快照保存可解释配置，hash绑定它的内容，恢复时可检测同一任务的配置漂移。workspace决定local-demo或百炼身份，因此profile选择必须发生在创建job时。


## 2026-11-11：M69 三个模型适配器的HTTP协议回归测试

为使真实云凭据缺失时仍能自动验证协议适配，新增基于`httptest`的Fun-ASR、Embedding、Qwen HTTP客户端测试，不访问公网、不使用真实密钥。

Fun-ASR测试覆盖异步POST路径、Bearer与异步头、单文件URL及channel参数、task/request ID解析；2xx缺task ID判为受理结果不明，429保留Retry-After，5xx标记结果不明；task查询字段映射和task ID一致性；转写URL仅允许HTTPS Aliyun主机及443端口，并拒绝无效字幕时间区间。Embedding测试核对模型、1024维、dense、query/document角色和文本顺序，响应index重排后能恢复原请求位置，并检查usage、429及Retry-After。Qwen测试核对固定快照、`enable_thinking=false`、非流式与零temperature；决策请求只带两个只读工具、禁用并行工具调用，并保留assistant tool_call与tool result的call ID；答案请求使用strict JSON Schema，响应usage及429字段可解析。

`TEST_DATABASE_URL=... go test -count=1 ./...`、`go build ./...`、`go vet ./...`均通过。已启用本地PostgreSQL集成测试；Kafka/PostgreSQL自动交接测试因未配置隔离broker环境变量`TEST_KAFKA_BROKERS`而跳过，以免与本机background争抢固定consumer group。上述HTTP测试用的是本地模拟服务器，只证明代码当前发/收格式和错误分类，不证明供应商实际接受这些字段；Fun-ASR/Embedding/Qwen真实地域、鉴权、schema和账号配额仍需云端联调。

面试问题：**没有真实密钥时，provider适配器怎样验证？** 用本地HTTP server断言method/path/header/body和返回映射、错误分类；这覆盖应用协议接线与状态解释，但server由自己编写，不能证明供应商服务端契约、模型行为或计费，真实账号小样例验收仍是独立证据。


## 2026-11-11：M70 上传对象与数据库确认丢失恢复

新增`TestUploadCompletionRecoversAfterOSSAndDatabaseAcknowledgementLoss`。测试使用独立临时PostgreSQL schema应用完整迁移，并通过`httptest`风格的对象存储替身注入两次“服务端副作用已成功、本地没有收到成功确认”：第一次OSS CompleteMultipart已生成带正确会话/视频metadata的对象，但返回错误；UploadService将会话置为VERIFYING。下一次Complete先HEAD对象并跳过重复合并，再实际调用FinalizeUpload提交blob、首个task、Outbox事件及COMPLETE会话；repository decorator随后模拟数据库COMMIT已成功但调用者收到连接丢失。再次Complete从COMPLETE记录读取原task，不重发OSS完成、不再最终化。

PostgreSQL最终状态断言：唯一task为version 1，唯一source blob、唯一Outbox事件，video.next_task_version=2/file_state=UPLOADED，upload.initial_task_id指向返回task且parse_slot_reserved=false。测试schema在结束时关闭连接后DROP，不碰开发schema；对象存储为模拟实现，不访问OSS。

本地`TEST_DATABASE_URL=... go test -count=1 ./...`、`go build ./...`、`go vet ./...`均通过。Kafka/PostgreSQL自动交接测试仍因未设置隔离`TEST_KAFKA_BROKERS`跳过。面试问题：**OSS合并成功但客户端超时，为什么恢复先HEAD而不是再次Complete？** 客户端错误不代表对象未生成；VERIFYING接管时先核对确定object key、大小和metadata，发现对象则直接进入数据库最终化，避免依赖OSS对已完成upload_id重试的行为。数据库提交回执再丢失时，upload session中的initial_task_id是重放锚点，保证不重复分配task_version或Outbox。


## 2026-11-11：M71 删除Worker阶段集成验收

新增`internal/background/deletion/worker_integration_test.go`。测试使用完整迁移的独立PostgreSQL schema，通过正式`DeleteVideo`用例创建PENDING删除任务；插入源blob，再由本地对象存储替身提供源对象和一个未登记的音频孤儿。测试实际执行Worker领取、WAIT_UPLOADS收敛、OSS对象版本扫描、GC台账登记、对象逐条删除、数据库分阶段清理、末轮扫描及墓碑提交。最终断言`video.lifecycle=DELETED`、job为`SUCCEEDED/DONE`、blob和GC台账均清空，两个对象版本都删除；初扫与末扫合计至少调用六次。

为保持测试快速，测试把`next_run_at`改为立即可领取，跳过正式24小时保护期；对象存储行为是本地替身，不验证真实OSS分页、删除标记或版本语义；未覆盖多Worker租约竞争、ASR远端等待与末轮扫描期间晚写竞态。测试schema清理，不碰开发schema。

`TEST_DATABASE_URL=... go test -count=1 ./...`、`go build ./...`、`go vet ./...`均通过。Kafka/PostgreSQL自动交接测试由于未设置独立`TEST_KAFKA_BROKERS`而跳过；未调用云服务。

面试问题：**为什么删除任务采用“对象台账→外部删除→数据库分批清理→末轮扫描→墓碑”的顺序？** 外部对象存储与PostgreSQL没有共同事务，先把确定的对象身份写入可恢复台账，再幂等执行删除；数据库逐表小批清理限制锁/WAL和回滚范围；末轮扫描找出并收敛在初次枚举后出现的对象；墓碑保留最少身份以阻止旧请求重放。


## 2026-11-11：M72 迟到ASR受理与删除门禁收敛

新增`internal/background/asrreconcile/worker_integration_test.go`，并将`asrreconcile.Worker`依赖从具体`*funasr.Client`收窄为`TaskQuerier`接口，便于隔离测试provider查询，不改变生产client行为。

测试在独立PostgreSQL schema使用ResourceService创建真实parse task，建立ASR `SUBMITTING`账本并由StageRepository记录提交结果不明。删除前查询得到`ASR_TASK_ID_UNAVAILABLE`；DeleteVideo后task被取消且run_epoch从1升到2；模拟旧epoch在删除提交后才拿到provider task ID并将其保存到账本。删除查询随即显示`ASR_REMOTE_RECONCILIATION_PENDING`。随后运行真实ASR reconciliation Worker/仓储，fake provider对准确task ID返回`CANCELED`；数据库验证submission为`REMOTE_TERMINAL`、保存provider request/status、reconcile epoch=1且租约释放，删除门禁变为settled。

`TEST_DATABASE_URL=... go test -count=1 ./...`、`go build ./...`和`go vet ./...`均通过；Kafka自动交接测试未配置独立broker而跳过。没有调用Fun-ASR、OSS或其他云服务。测试证明本地状态机能承接迟到task ID并等待明确终态，但不证明供应商取消任务语义、查询接口真实状态映射、账单或多实例竞争。

面试问题：**视频删除时 ASR POST 还没有返回，后来才拿到task_id怎么办？** 删除先冻结视频并撤销parse task执行epoch，但不能丢弃已经发出的外部请求。迟到响应仍追加到独立submission账本；删除门禁从“task_id未知”转为“远端任务对账中”；独立reconciler按自己的lease/epoch查询，只有供应商明确终态才放行对象和数据库GC。业务执行epoch负责阻止旧Worker提交解析结果，reconcile epoch负责隔离远端查询者，两者作用不同。

## 2026-11-11：M73 ASR自动补交预算跨epoch验证

新增`TestASRSupplementBudgetSurvivesWorkerEpochChanges`。使用真实StageRepository预留初次提交，记录UNKNOWN后通过数据库时间控制跨过五分钟缓冲；Manager以run_epoch=2接管，仅生成submit_seq=2。第二次提交被记录为429拒绝并进入WAITING，run_epoch=3再次接管后，第三次预留得到`ErrASRSubmissionBudget`。最终断言只有两条submission、task_version仍为1、run_epoch为3，证明重试/接管不会清空外部副作用预算。

本轮`TEST_DATABASE_URL=... go test -count=1 ./...`、`go build ./...`和`go vet ./...`均通过；Kafka自动交接测试未配置独立broker而跳过。未调用Fun-ASR、OSS或其他云服务；五分钟等待用时间字段注入，不代表真实延迟。

面试问题：**为什么外部提交次数不能跟着Worker run_epoch重置？** run_epoch只标识当前有权提交解析检查点的执行者；ASR POST是不可由数据库回滚的外部副作用，预算必须绑定逻辑task/submission账本。接管时读取同一个账本，因此epoch可递增、补交最多一次的业务限制不变。

## 2026-11-11：M74 Kafka与PostgreSQL隔离集成交接

检查之前因固定consumer group未运行的`TestOutboxKafkaInboxOffsetHandoff`。首次在临时Kafka broker上运行时，它把样例任务写入共享开发schema；常驻background抢先领取任务，测试观察到RUNNING而误以为Inbox没有推进。这暴露测试环境与运行服务共享数据库造成的竞态。

将Kafka集成测试改为创建随机隔离PostgreSQL schema、应用完整迁移，并在测试结束关闭连接后DROP；持久数据不再暴露给常驻API/background。启动临时Apache Kafka 4.1.1 broker绑定127.0.0.1:19092，与项目常驻Kafka 9092隔离。`TestOutboxKafkaInboxOffsetHandoff`通过：Outbox事件发送至Kafka、Inbox落库后任务变READY，重复处理未生成重复Inbox行，Outbox状态为SENT。

本轮在同时设置`TEST_DATABASE_URL`与`TEST_KAFKA_BROKERS=127.0.0.1:19092`时运行全量`go test -count=1 ./...`通过，`go build ./...`及`go vet ./...`通过。临时broker随后停止；项目原有Kafka、数据库及常驻consumer未被加入/重启。该测试证明单broker本地交接和重复事件路径，不覆盖Kafka重平衡、broker故障恢复或多实例压力。

面试问题：**为什么Kafka集成测试也要给PostgreSQL建隔离schema？** 就算Kafka consumer group独立，共享数据库任务仍会被真实background worker领取，导致测试观察值被另一进程改变。隔离Kafka和数据库两侧，才能把测试写入限制在专用状态空间，并避免清理时误删开发数据。

## 2026-11-11：M75 当前源码完整本地纵向链路验收

为避免复用停用的开发容器与数据卷，使用唯一名称启动临时PostgreSQL 17/pgvector和Kafka 4.1.1：PG仅映射127.0.0.1:25432并使用`mirrorcore-e2e-pgdata-20261111`卷；Kafka仅映射127.0.0.1:29092。当前源码API/background分别使用隔离端口28080/29091/29093，数据库中仅包含本次演练用户和夹具。退出后已停止并移除这两个容器、隔离卷和工作区scratch目录；原项目停用容器与数据卷未启动、未修改。

设置`LOCAL_DEMO_MODE=true`和无云凭据配置，运行`cmd/seed-media-demo -fixture-key active-goal-e2e-20261111`。`parse_tasks`结果为`SUCCEEDED`、task_version=1、run_epoch=8；PROBE、AUDIO、ASR、TRANSCRIPT、CHUNK、EMBEDDING、PUBLISH七个`task_steps`均为READY。内容Worker将该结果的revision 1推进至`SUCCEEDED/DONE`。这里的MP4为合成媒体，ASR、Embedding和内容生成均为固定本地实现。

当前源码API随后实跑登录/CSRF、会话创建、问题SSE和结果读回。SSE依次包含accepted、5个progress、answer、done；`GET /v1/questions/{id}`确认执行状态SUCCEEDED且答案已持久化。接着运行`cmd/qa-strategy-smoke`：固定RAG与Agent锁定同一result_id并取得相同初始句序号[1,2]；Agent发起一次无新增证据的工具调用后结束。报告明确标记`synthetic_fixture`，其本地耗时不代表provider/生产延迟，不提供语义质量或费用结论。

第一次用`MEDIA_TEMP_DIR=/tmp/...`启动background时，macOS Podman VM无法挂载该host路径，media工具版本探测失败（`statfs ... no such file`）。按README改为位于共享工作区的唯一scratch路径后，media processor通过版本探测，PROBE和AUDIO真实调用FFmpeg容器成功。结论：容器化本地媒体演练的scratch路径必须位于Podman VM共享目录；示例使用工作区路径。

本轮未调用OSS、Fun-ASR、Embedding或Qwen真实服务，也未运行生产压力/多实例验证。M75证明本地当前源码在隔离依赖上完成解析、内容和问答的组合链路，不替代真实云服务或质量评估。

面试问题：**本地纵向smoke证明了什么、没有证明什么？** 它证明当前API、PostgreSQL、Kafka、Outbox/Inbox、媒体Worker、内容Worker、pgvector查询、Agent编排、SSE和答案提交能在一个隔离环境中组成完整控制流；替身返回固定内容，所以不能证明Fun-ASR识别质量、百炼协议兼容、模型回答/引用质量、计费或生产可用性。

面试问题：**为什么这条解析任务的task_version为1、run_epoch为8？** task_version标记用户业务解析版本；run_epoch标记执行租约代次。七个阶段都要分别领取和提交检查点，ASR还发生一次WAITING后再次领取，因此同一业务版本可由多个执行代次完成，不会因接管就创建新task_version。

面试问题：**Podman挂载为什么建议用工作区scratch路径？** 容器实际运行在macOS虚拟机中，host路径必须处于VM共享目录。host上能创建`/tmp`不代表VM看得见它；使用已共享的`/Users/...`工作区路径可让容器读写同一scratch数据。


## 2026-11-11：接手审查与剩余工作收敛

本轮按当前源码、迁移、进程入口、B01至B16要求与历史运行记录重新核对；没有仅依赖里程碑摘要判断完成。`go test -count=1 -v ./...`、`go build ./...`和`go vet ./...`均退出0；10个依赖PostgreSQL/Kafka的集成用例因本轮未配置测试地址而SKIP。没有重启Podman/数据库/Kafka，也没有云请求。本轮结果不覆盖M74集成测试或M75纵向链路的重新执行。

发现此前“主要只差云端验收”的说法遗漏真实代码缺口：存活视频下的历史问答/轨迹、旧结果与无引用产物保留期回收没有调度器；现有清理仅覆盖视频删除。scratch依赖defer，强制进程退出后的遗留目录未被定期扫描；媒体进程组退出保障需要专项确认。QA、chunk、媒体、内容和配额核心边界缺少直接自动回归，历史手工演练并不构成持续回归保护。真实上传到引用播放闭环、人工评估数据、可复现交付入口仍未完成。完整代码证据、优先级和完成判定已写回主设计文档“接手核查：实际剩余项”。

模型决策保持不变。官方[模型更新记录](https://help.aliyun.com/en/model-studio/newly-released-models)仍列出已选的qwen3.7-plus-2026-05-26；[调用说明](https://help.aliyun.com/en/model-studio/stream)给出北京workspace专属compatible-mode地址。它们支持保留当前选型，不能代替本账号下对工具调用、严格Schema和非思考参数的真实请求验收。

修正主设计当前表格中“没有内容Worker”的过期描述：M32/M75已有本地执行证据；将旧常驻进程时间说明标记为历史观察。`.gitignore`新增已有`.localdemo-current-source`运行产物目录，保留目录内容。Git尚无提交，本轮未自动提交源码。

面试问题：**删除视频的GC和历史保留期GC为什么要分别设计？** 前者先冻结整个video，取消所有新引用；后者处理仍在使用的视频，必须保护当前结果、运行任务、未过期问答和跨任务复用产物，不能按创建时间直接删文件。

面试问题：**为什么defer不能保证临时文件总会清理？** SIGKILL、主机掉电等情况不会运行defer；需要用task/epoch目录身份、数据库执行资格与安全窗口识别遗留文件，保守清理无有效执行者的目录，并保留活跃处理所需的数据。


## 2026-11-11：百炼受限Key配置与OSS开通交接

用户授权在右侧浏览器配置云服务，并在提交前确认创建专用长期API Key、IP保持默认放通及保存到本机项目`.env`。已在北京默认业务空间创建MirrorCore专用Key；重新打开编辑页核对持久化模型权限恰为`fun-asr`、`text-embedding-v4`和`qwen3.7-plus-2026-05-26`三项，没有更改旧Key。新Key及workspace已写入本机`.env`，文件权限0600并受Git忽略；本文不保存Key、账号ID或完整签名信息。浏览器复制按钮的剪贴板结果为空，后续直接从可见的一次性密钥对话框读取并安全保存，保存后关闭对话框；聊天输出始终脱敏。

仅完成凭据配置，没有实际模型调用；Embedding/Qwen的四个RPM/TPM参数仍留空，不能据此报告服务可调用或问答效果。OSS控制台明确显示账号未开通服务、开通后默认按量付费；官方开通入口在内置浏览器出现连接关闭，尚未接受服务协议、开通服务、创建Bucket或创建存储凭据。已保留OSS控制台供用户接手完成开通；之后继续配置Bucket、限定存储权限、模型额度与真实单视频联调。

本轮使用`sh -n .env`验证配置语法，并仅以布尔状态检查所需字段是否配置，未输出密钥；没有运行应用、测试或云端模型请求。配置成功截图与OSS待开通截图位于被忽略的本机`tmp/`目录。

## 2026-11-11：OSS限定授权、分片生命周期与百炼限流配置完成

用户完成OSS开通后，通过控制台创建北京Bucket `mirrorcore-dev-20261111-b7f6`，标准存储/LRS、私有ACL、阻止公共访问与版本控制开启。用户明确确认RAM用户`mirrorcore-dev`、永久AccessKey、限定授权及本地保存，并在阿里云页面自行完成用户创建和策略保存的两次身份验证。自定义策略`MirrorCoreOSSDevStorage`创建后绑定成功，页面显示1个操作成功、0个失败；不启用RAM控制台登录。

策略清单为`deploy/ram/mirrorcore-oss-policy.json`：仅允许该Bucket的`oss:ListObjectVersions`；对象操作限定在`source/*`、`artifacts/audio/*`、`artifacts/asr/*`，包含Put/Get/GetObjectVersion、Delete/DeleteObjectVersion、ListParts和AbortMultipartUpload。永久Key保存至Git忽略的`.env`，权限0600；下载的凭据CSV核对保存后删除，没有将密钥写入日志或文档。

用户确认不可恢复分片清理后，保存`mirrorcore-abort-incomplete-source-uploads`规则；列表显示启用中，`source/`下超过3天的未完成分片将清理。清单为`deploy/oss/abort-incomplete-multipart.xml`。完整文件、历史版本、删除标记均不配置过期；控制台提示48小时内生效，实际生命周期执行未验证。

按北京百炼模型详情核对并写入`.env`：`text-embedding-v4`为1800 RPM/1200000 TPM，`qwen3.7-plus-2026-05-26`为600 RPM/1000000 TPM。主干`qwen3.7-plus`显示30000 RPM/5000000 TPM，切换固定快照后显示不同值，因此采用快照值。以上是模型详情页的配置依据，不是实际账户压力/429实验结论。

本轮检查`.env`语法、必填状态、0600权限与Git忽略，并核对JSON/XML清单；同步README、交接与主设计文档及桌面副本。云端结果截图保存在本机忽略目录`tmp/`。没有启动应用、运行Go测试、上传/下载/删除真实OSS对象，或调用ASR/Embedding/Qwen。真实业务闭环仍需允许上传处理的视频与后续运行验收。

面试追问：**为什么不直接按3天自动清理历史对象版本？** 已发布结果和问答可能仍绑定旧版本；先按引用/执行围栏决定可回收对象，再删除指定版本，才能保持可恢复与可追溯。未完成分片使用独立上传窗口清理，超时未完成的上传需重新开始。**为什么限流按固定模型ID配置？** 主干与快照可有不同限制；配额门闩必须匹配实际调用身份，仍需处理真实429和供应商冷却。


## 2026-11-11：视频问答界面交互预览

用户希望先看到前端界面。仓库仍只有Go后端，未发现前端工程或现成页面；本轮制作聊天内可点击原型，包含视频列表、示例上传、七阶段解析、概览/字幕切换、示例问答及时间戳定位。预览源位于`/Users/jiaming.dong/.codex/visualizations/2026/11/10/01a1c414-03b1-7492-8c41-7f9b4864cd0a/mirrorcore-interface-preview.html`。

浏览器检查了示例回答、引用跳转保留答案、添加示例视频、逐阶段推进至完成后开放问答；JavaScript语法与片段检查通过。它不连接后端、不上传文件或调用模型，所显示内容均明确标为演示。此预览不构成正式前端交付，也不改变真实云端链路待验收的状态。


## 2026-11-11：整理到指定GitHub仓库

用户指定`https://github.com/MOLI0711/VedioAgent`同步整个MirrorCore项目及交互预览。本地原为无提交仓库；读取远程main现有历史后，在其上建立`codex/mirrorcore-import-20261111`分支，保留原有开发规范、H4任务书和后端图。通用规范双副本同步新增实际目录职责，README增加当前任务书、演示边界与静态预览入口。

预览可编辑片段与独立浏览器导出页存入`web/preview/`；它不连接真实后端。`.gitignore`补充个人输出目录、临时工作目录、日志和系统文件，排除已有简历PDF；云端.env、密钥、运行截图、媒体scratch及二进制均不提交。提交前扫描实际本机凭据的精确匹配和常见凭据模式，不输出密钥。

本轮`go test -count=1 -v ./...`、`go build ./...`、`go vet ./...`全部退出0，10个PostgreSQL/Kafka集成测试因未配置依赖而SKIP；没有启动数据库/Kafka或调用云模型。预览JavaScript语法与HTML导出检查通过。同步采用新分支，保留远程main；不强推、不部署。最终推送结果以远程分支与本地提交SHA一致为确认依据。

推送确认：首次源码提交`fadb289`已到达`origin/codex/mirrorcore-import-20261111`，通过`git ls-remote`核对完整SHA与本地一致；当时工作区干净。随后同步主设计的Git状态与桌面副本，保留main及既有文档历史。
