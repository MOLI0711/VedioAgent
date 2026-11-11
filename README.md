# MirrorCore 后端

按 `MirrorCore_AI_Vibe交接文档.md` 的里程碑开发。后端采用 Go/Gin、PostgreSQL/pgvector 与 Kafka；源视频、音频及原始转写使用 OSS；ASR、Embedding 和生成模型通过百炼 API 接入。`web/preview/`提供可点击的界面原型，尚未连接后端；正式前端仍未开发。

## 先看界面预览

下载仓库后，可直接用浏览器打开[界面预览](web/preview/index.html)，或在仓库根目录启动静态服务：

```sh
python3 -m http.server 5173 --bind 127.0.0.1 --directory web/preview
```

然后访问`http://127.0.0.1:5173/`。可体验视频列表、添加视频、解析进度、内容概览、字幕、示例问答与时间戳定位。预览只使用演示数据，不上传视频、不调用模型，不代表真实业务联调完成；界面图标和预览运行样式需要网络加载。可编辑片段为`web/preview/mirrorcore-preview.fragment.html`，`index.html`为独立浏览器导出版。

## 项目文档

- [当前Go后端与Agent任务书](MirrorCore_AI_Vibe交接文档.md)
- [设计、问题与面试记录](MirrorCore项目设计与问题记录.md)
- [开发日志与验证边界](docs/development-log.md)
- [早期H4设计参考](backend-agent-development.md)与[原始后端图](backend-architecture.png)

本仓库继承已有设计文档历史；本机`.env`、云端凭据、个人简历、截图、日志和运行产物不进入提交。克隆仓库后需自行配置本地环境，不能使用本工作区已配置状态作为其他机器的启动前提。

## 当前实现与验收边界（2026-11-11）

核心Go API、Kafka任务交接、分阶段Worker、任务版本发布、检索/Agent问答、SSE和离线评估工具已实现。合成媒体经本地PostgreSQL/Kafka从PROBE跑到PUBLISH；本地Agent问答经HTTP/SSE返回并持久化。SIGTERM、客户端断连及“成功答案提交与取消同时发生”的本地数据库竞态均有运行证据，详见[设计与问题记录](MirrorCore项目设计与问题记录.md)和[开发日志](docs/development-log.md)。本机数据库已应用`000001`至`000022`迁移；这些是本工作区的历史运行记录，其他机器仍需按启动步骤建立环境。

云端准备已完成：MirrorCore专用百炼Key限定Fun-ASR、text-embedding-v4和已选Qwen快照；北京私有OSS Bucket `mirrorcore-dev-20261111-b7f6`已创建，开启版本控制与阻止公共访问。RAM用户`mirrorcore-dev`只绑定项目限定策略，Key及存储凭据已写入本机`.env`（0600、Git忽略）。按百炼模型详情页填写Embedding 1800 RPM/1200000 TPM、Qwen固定快照600 RPM/1000000 TPM；这些是当前控制台显示值，尚未通过真实请求核验账户实际行为。OSS已保存仅清理`source/`下超过3天未完成分片的生命周期规则。尚未发起真实模型或OSS业务上传/下载/删除；真实分片上传、供应商协议兼容、识别/生成质量及生产压力仍待联调验收。

## 本地启动

需要 Go 1.27.1+、Podman Compose（也可用兼容的 Docker Compose），以及运行媒体 Worker 所需的 `ffmpeg`/`ffprobe`。仅在尚无`.env`时复制模板，把`BOOTSTRAP_PASSWORD`换成只用于本地的值。本工作区已经保存云端配置，不要用模板覆盖。启动依赖并运行API：

```sh
test -f .env || cp .env.example .env
podman compose up -d
set -a && . ./.env && set +a
go run ./cmd/api
```

检查 `GET /healthz` 与 `GET /readyz`。API和background指标端点分别默认监听`127.0.0.1:9091/metrics`与`127.0.0.1:9093/metrics`；Podman Compose会同时启动本地Prometheus，界面地址为`http://127.0.0.1:9090`，配置文件在`monitoring/prometheus.yml`，告警规则在`monitoring/alerts.yml`。默认地址只供本机访问；内网抓取时用`API_METRICS_ADDR`和`BACKGROUND_METRICS_ADDR`配置私有地址并限制网络访问，不要暴露到公网。API与background启动时都会在advisory lock保护下应用未执行的数据库迁移。默认指标地址避免与本地Kafka的9092冲突；LOCAL_DEMO_MODE会拒绝非回环地址。另开一个终端，加载同一份 `.env` 并启动后台进程：

```sh
set -a && . ./.env && set +a
go run ./cmd/background
```

后台进程会确保本地Kafka主题存在，运行Outbox投递、Inbox消费和任务超时维护。配置OSS时还会运行上传取消/过期回收，并启用可恢复的TRANSCRIPT处理器；同时安装`ffmpeg`/`ffprobe`后会启动PROBE/AUDIO处理器。配置百炼workspace/API Key后会启用Fun-ASR与text-embedding-v4处理器。CHUNK阶段使用Go内置的`chunk_v1_bytes`，不依赖外部tokenizer文件；Embedding以每批最多10条同步请求并将向量及预算账本写入PostgreSQL/pgvector。所有阶段共用两个Worker槽位；长时间等待ASR时会持久化WAITING并释放本地槽位。媒体处理临时目录按任务/执行epoch隔离，准入时保留10GiB空闲空间、每个媒体任务预留最多3GiB。

上传接口只有在 `OSS_BUCKET` 与 `OSS_REGION` 配置后才启用。默认 `OSS_CREDENTIAL_MODE=env` 使用 `OSS_ACCESS_KEY_ID`、`OSS_ACCESS_KEY_SECRET`，临时凭据还需 `OSS_SESSION_TOKEN`；部署到带RAM角色的ECS时可改为 `ecs`。不要把密钥写入仓库或前端。Bucket应保持私有，并配置未完成MultipartUpload生命周期清理；浏览器直传启用后还需将CORS限制到实际前端来源。可参见[OSS Go SDK V2凭据配置](https://help.aliyun.com/zh/oss/developer-reference/manual-for-go-sdk-v2/)和[未完成分片生命周期清理](https://help.aliyun.com/en/oss/user-guide/overview-54/)。本工作区的凭据和生命周期规则已配置，真实OSS上传尚未联调。限定授权清单见`deploy/ram/mirrorcore-oss-policy.json`，分片清理清单见`deploy/oss/abort-incomplete-multipart.xml`；规则不清理完整文件、历史版本或删除标记。

登录接口为 `POST /v1/sessions`，传入 `{"email":"demo@mirrorcore.local","password":"本地配置的密码"}`。会话使用 HttpOnly Cookie，写接口还需要在 `X-CSRF-Token` 请求头中带回登录响应的 CSRF token。API契约通过`GET /openapi.yaml`提供，源文件位于`internal/httpapi/openapi.yaml`；Q&A单次执行最长180秒，HTTP读取超时30秒、写入超时210秒，为SSE终结事件留出收尾时间。

`GET /healthz`只检查进程；`GET /readyz`检查数据库。配置OSS后，用户可经上传会话创建视频：Go发放分片URL，完成接口核对OSS分片和对象元数据，再于同一PostgreSQL事务创建源对象引用、首个解析任务和Outbox事件；DELETE上传会话会异步回收分片。后台已能把Outbox事件投递到Kafka，可靠写入Inbox后将任务置为READY。安装媒体工具且配置OSS时，Worker可执行PROBE与AUDIO并恢复READY音频产物；配置百炼workspace/API Key后可继续执行Fun-ASR，持久化提交预算、远端task_id、轮询等待和OSS原始结果，再经TRANSCRIPT处理器生成视频时间轴字幕，按每批200句保存连续位点并发布完整检查点。CHUNK处理器使用`chunk_v1_bytes`按UTF-8字节切分，目标512字节、硬上限768字节、仅重叠不超过128字节的完整句子，保留字幕来源范围；集合可恢复或按配置复用。EMBEDDING通过百炼原生同步HTTP API调用text-embedding-v4，固定1024维稠密向量，每个稳定序号批次最多10条；请求前在数据库预留最多3次预算，校验完整响应后将pgvector数据和批次账本在同一epoch围栏事务提交，完整集合发布后推进到PUBLISH。PUBLISH现已实现：核对字幕、片段、向量及来源完整性，保存不可变解析结果清单；锁定视频后只在任务版本高于当前生效版本时更新`active_result_id`，旧版本完成也不会覆盖新结果。固定版本问答和受控Agent已有Go实现：问答受理时固定result_id，按该embedding集合检索；Agent只允许search_video/read_context，并在答案结构和来源校验、数据库保存后才通过SSE发送完整答案。本机PostgreSQL 17/pgvector已应用`000001`至`000022`全部迁移；离线夹具走通登录、会话、pgvector检索、固定`search_video`处理、答案结构/证据校验、落库及SSE。未进行真实百炼或OSS调用。删除回收新增持久化清理阶段、按video_id枚举OSS对象版本的首轮/末轮扫描，以及与解析任务生命周期解耦的Fun-ASR远端对账。已知task_id且不再由解析Worker处理的ASR作业（取消/删除、被更高submit_seq替代，或SUCCEEDED/CANCELLED/不可继续FAILED终态）由后台reconciler查询，供应商确认终态才解除对应等待；提交响应即使与本地取消竞态，也会保存到ASR账本。UNKNOWN且task_id不可得时不猜测远端状态，删除任务保持等待并返回`blocking_reason=ASR_TASK_ID_UNAVAILABLE`。迁移已应用；M17增加Embedding/Qwen共享RPM/TPM准入，M18增加初步Prometheus指标，M19加入本地抓取与pgxpool、数据库队列快照，M43补充终态内容任务告警，M46接入Kafka consumer group lag和offset过期检测；M48-M56在本机或隔离环境验证了数据库快照、Kafka lag、Outbox/DLQ、ASR/删除、配额清理、API/pgxpool和本地假provider告警触发。Prometheus已抓取API和background；这些本地故障输入不代表真实百炼下的provider告警或生产压力。没有真实百炼额度/usage样例验证，数据库删除阶段曾用临时记录跑过31次PurgeBatch调用，501条GC账本按500+1清理并完成视频墓碑；OSS版本列表只模拟空页且绕过24小时保护期，不代表真实对象删除。真实OSS删除和百炼对账仍未运行。托管text-embedding-v4的等价本地tokenizer未验证，因此byte count不是模型token count，也不宣称是供应商限制的严格上界。主机未直接安装`ffmpeg`/`ffprobe`；M34已通过Podman容器运行合成媒体。真实OSS上传、Fun-ASR、云Embedding/生成、内容模型和对象删除仍未实测。不要将本地演示账户或Compose密码用于公开部署。

百炼接入使用`.env.example`中的`BAILIAN_REGION=cn-beijing`、`BAILIAN_WORKSPACE_ID`和`BAILIAN_API_KEY`。启用百炼时，还必须按供应商控制台实际额度填写`BAILIAN_EMBEDDING_RPM/TPM`与`BAILIAN_QWEN_RPM/TPM`，API与background必须使用相同配置。PostgreSQL按workspace/model共享滚动60秒准入，成功后用供应商usage修正保守token预留；失败请求也计RPM，429的Retry-After会触发跨进程冷却。这是应用侧协调，不替代供应商配额或真实账户验证。百炼密钥只放在后台进程环境中；服务端不会记录签名音频URL。调用前需配置可生成至少覆盖ASR等待窗口的OSS签名凭据；临时凭证剩余时长不足时会拒绝提交。

### 处理没有provider task_id的ASR UNKNOWN

仅当供应商控制台或支持工单已确认该提交终态或从未受理后，才运行运维命令；该命令不会访问供应商，也不会把本地超时当作远端终态。它只允许处理没有`provider_task_id`的UNKNOWN提交，且解析任务必须已进入SUCCEEDED/CANCELLED终态、已不可人工继续的FAILED终态，或视频处于删除流程；运行中或仍可人工继续的解析任务不能人工解除门闩。操作者、供应商终态、证据引用和原因会与`REMOTE_TERMINAL`转换在同一数据库事务内保存。若供应商受理响应随后迟到并带回task_id，系统会将submission恢复为ACCEPTED、标记旧人工处置被取代，并交给正常reconciler继续查询；相同参数重放不会重复写审计。

```sh
go run ./cmd/asr-unknown-resolve \
  --submission-id "$ASR_SUBMISSION_ID" \
  --operator "$OPERATOR_ID" \
  --terminal-status NOT_ACCEPTED \
  --evidence-ref "provider-case:CASE-123" \
  --reason "供应商确认该请求未创建远端任务" \
  --confirm I_VERIFIED_PROVIDER_TERMINAL
```

`--terminal-status`可填`SUCCEEDED`、`FAILED`、`REJECTED`、`CANCELED`、`CANCELLED`、`EXPIRED`或`NOT_ACCEPTED`。若稍后收到迟到的受理响应，系统会撤销人工终态、恢复`ACCEPTED`并交给reconciler；命令重放同一审计参数是幂等的。数据库凭据本身是该运维命令的访问边界，部署时应使用受控运维身份，不通过普通用户HTTP接口暴露。

Fun-ASR未知受理且没有`task_id`时，系统无法查询特定远端作业。当前将五分钟等待作为一次性补交前的缓冲窗口，不把它称作远端核实；最多两条提交记录共同限制为初始提交加一次自动补交。若被补交替代的旧提交迟到返回`task_id`，reconciler会继续跟踪旧任务至供应商终态；任务SUCCEEDED/CANCELLED、不可人工继续的FAILED或视频删除后才收到`task_id`的提交也会对账，避免遗留任务长期占用并发名额。百炼查询接口以`task_id`作为路径参数；接口文档没有描述按本地请求ID查询或客户端幂等键。[Fun-ASR HTTP API](https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-http-api)。

已实现的资源接口需要登录：`GET /v1/videos`、`GET /v1/videos/{id}`、`DELETE /v1/videos/{id}`、`GET /v1/video-deletions/{id}`、`GET /v1/parse-tasks/{id}`与`GET /v1/parse-tasks/lookup`，以及解析任务创建、人工继续和取消接口。删除视频与解析任务取消会在同一事务内同步终止活动阶段检查点。人工继续仅允许可恢复失败任务一次；沿用原`task_version`，保存重试请求键与审计快照，不重置ASR提交预算。还包括`POST/GET /v1/uploads`、上传会话lookup、分片签名、complete和DELETE取消接口。写接口要求`X-CSRF-Token`；任务创建与待投递Outbox事件在同一数据库事务提交。当前只允许`voice-v1` profile，重建参数见项目设计文档。本机数据库当前已应用`000001`至`000022`迁移。

## 固定版本问答与 Agent

需要先让视频解析出完整的TEXT结果。登录后调用`POST /v1/videos/{id}/conversations`创建会话，然后`POST /v1/videos/{id}/questions`发起问答；请求必须带`Idempotency-Key`和`X-CSRF-Token`，响应为SSE。连接事件包括`accepted`、`progress`、通过验证并持久化后的`answer`以及`done`；可用`GET /v1/questions/{id}`查询状态，`POST /v1/questions/{id}/cancel`取消运行中问答。会话历史由`GET /v1/conversations/{id}/questions`读取，删除会话接口执行归档。

Agent最多调用`search_video`和`read_context`两个只读工具，在受理时固定的结果和字幕范围内补查；答案正文通过Schema和证据编号检查并保存后才推送。`BAILIAN_WORKSPACE_ID`与`BAILIAN_API_KEY`未配置时，非空语音结果的问答会明确失败，不返回演示答案。百炼官方文档对所选qwen3.7-plus快照能否设置`enable_thinking=false`存在冲突，当前实现保留已确认配置，需真实账户验证。

### 本地离线后端纵向演示

为了练习本地 Go 后端链路，可以显式启用`LOCAL_DEMO_MODE=true`。该模式要求API、PostgreSQL和所有Kafka broker都绑定回环地址，并拒绝同时配置OSS或百炼凭据。`seed-demo`会创建固定视频和首版结果，再通过真实Outbox/Kafka/Inbox投递一个`task_version=2`任务；任务从已持久化的合成字幕检查点开始，由生产Worker实际执行CHUNK、写入明确标记为`local-demo`的确定性1024维Embedding，并运行PUBLISH版本切换。随后可通过真实API走到pgvector检索、固定Agent、答案校验、数据库落库和SSE。Embedding及答案均为本地替身，不调用云服务。

该切片不验证上传、真实视频/FFmpeg、OSS、Fun-ASR、百炼模型调用或语义生成质量；合成转写不是ASR结果，placeholder source blob也不是可播放媒体。正式链路仍按已确认设计使用OSS与百炼API。此模式只为本机学习状态交接、Worker阶段处理、版本发布和问答链路服务，不代表生产配置。

M75使用独立PostgreSQL 17/Kafka 4.1.1环境在当前源码上复核了完整本地链路：合成MP4经PROBE至PUBLISH（7个检查点READY），内容revision 1完成；再通过真实本地HTTP API登录、创建对话、接收问答SSE并从查询接口读回SUCCEEDED答案。固定RAG/Agent配对smoke确认两者使用同一结果版本和相同初始检索。该证据只证明本地组件可组合运行；ASR、Embedding、内容和问答生成均为确定性替身，不代表云端兼容或模型质量。macOS上的Podman若需容器挂载媒体scratch目录，应使用共享的工作区路径（如下方`$(pwd)/.localdemo-media-tmp`），不要假设`/tmp`自动共享给虚拟机。

### 合成媒体到解析结果的本地链路

除了从既有TRANSCRIPT检查点开始的seed-demo，seed-media-demo可用合成MP4创建独立的task_version 1任务。任务经Outbox、Kafka、Inbox后由现有Worker依次执行PROBE、AUDIO、本地确定性ASR、TRANSCRIPT、CHUNK、本地Embedding和PUBLISH。ASR不识别音频，只返回固定文本；所有源视频、音频和原始JSON均写到local-demo文件存储。该模式不调用OSS或百炼，也不会改变正式的OSS＋Fun-ASR方案。

AUDIO产物对象键由视频、输入指纹、提取profile和任务ID确定。若对象写入成功但PostgreSQL检查点未提交，重领任务会先HEAD并下载该对象，校验版本、metadata、SHA-256和WAV规格后补写检查点，避免重复执行FFmpeg。若对象身份或内容校验失败，当前任务会明确失败；不可变对象不被覆盖，后续需通过新任务版本重新解析并由对象GC清理孤儿。

先构建本地媒体工具镜像（首次构建需要容器镜像仓库网络访问）：

```sh
podman build -t mirrorcore-media-tools:local -f Containerfile.media-tools .
```

在加载.env的API和background终端中设置相同的本地工具路径与scratch目录：

```sh
export LOCAL_DEMO_MODE=true
export MEDIA_TEMP_DIR="$(pwd)/.localdemo-media-tmp"
export FFMPEG_PATH="$(pwd)/scripts/localdemo/ffmpeg"
export FFPROBE_PATH="$(pwd)/scripts/localdemo/ffprobe"
export MEDIA_TOOLS_CONTAINER_IMAGE=mirrorcore-media-tools:local
```

再启动依赖、API和background，并在另一个终端执行：

```sh
set -a && . ./.env && set +a
export LOCAL_DEMO_MODE=true
export MEDIA_TEMP_DIR="$(pwd)/.localdemo-media-tmp"
export FFMPEG_PATH="$(pwd)/scripts/localdemo/ffmpeg"
export FFPROBE_PATH="$(pwd)/scripts/localdemo/ffprobe"
export MEDIA_TOOLS_CONTAINER_IMAGE=mirrorcore-media-tools:local
go run ./cmd/seed-media-demo
```

重复执行会校验并复用同一video/task，不回退进度。重建素材可运行scripts/localdemo/generate-voice-demo.sh（需要macOS say）。当前容器素材是TTS合成语音；ASR固定结果不会读取音频，因此这条链路只验证媒体处理、对象边界、Kafka交接、阶段检查点及发布，不代表真实识别、供应商兼容或生产性能。

如需并行或重新演练一条独立任务，可传入稳定夹具键，例如`go run ./cmd/seed-media-demo -fixture-key interview-run-2`。同一个键重复执行会复用原video/task；不同键会创建独立video/task。默认`media-v1`保留原有固定身份。

`LOCAL_DEMO_MODE`也启动独立内容 Worker。登录后可对当前 TEXT 结果调用`POST /v1/results/{result_id}/content-jobs`创建 revision，使用`GET /v1/content-jobs/{job_id}`查询，并用`GET /v1/results/{result_id}/content`读取发布的概览和章节。内容任务使用`local-demo/local/deterministic-content-v1` profile；fixture只复制固定字幕来源并返回符合Schema的结构，不模拟Qwen语义总结。它会真实经过内容任务租约、批次尝试账本、来源校验与artifact发布，但不验证百炼生成质量。

先启动本地依赖、复制`.env.example`，并在`.env`中设置`LOCAL_DEMO_MODE=true`、`HTTP_ADDR=127.0.0.1:8080`、`COOKIE_SECURE=false`及本地演示账户；OSS/百炼凭据保持为空。

在终端1种入固定视频和pipeline任务：

```sh
set -a && . ./.env && set +a
go run ./cmd/seed-demo
```

在终端2启动API，在终端3启动background；两个终端都要加载同一份`.env`：

```sh
set -a && . ./.env && set +a
go run ./cmd/api
```

```sh
set -a && . ./.env && set +a
go run ./cmd/background
```

background会通过Outbox/Kafka/Inbox接收任务，完成后再登录`POST /v1/sessions`并调用`POST /v1/videos/{video_id}/questions`。示例问题为“这个视频里 Kafka 和执行 epoch 分别起什么作用？”。固定Agent执行一次`search_video`，发现没有新增证据后收尾；回答前缀标出“本地离线演示固定答案”。重复运行`seed-demo`会复用并校验同一组固定数据和pipeline任务；若检测到夹具被改坏或版本不一致，会停止而不是覆盖数据库内容。关闭模式只需移除`LOCAL_DEMO_MODE`或设为`false`；云端真实调用仍需另行配置供应商。
登录用户可通过`GET /v1/videos/{video_id}/playback`获取私有OSS签名播放请求；可同时提供`question_id`和`evidence_id`，服务端核验问答引用归属并返回`start_ms`/`end_ms`；也可提供`content_artifact_id`与`content_ref_id`以定位当前内容产物来源。签名有效期为2小时以支持长视频Range读取；客户端不能传对象键或任意URL。

预置账户仅在数据库中不存在时创建。修改 `.env` 的 `BOOTSTRAP_PASSWORD` 不会覆盖已有账户密码；重置本地演示数据时可删除本地PostgreSQL volume。生产部署须设置 `COOKIE_SECURE=true` 并从受控密钥系统注入凭据。


## 内容概览与章节

解析结果发布时，系统在同一事务为有语音的 TEXT 结果创建首个内容 revision。独立的内容 Worker 基于固定转写字幕生成概览和章节；批次计划、模型调用预算、租约、来源范围和内容 head 都持久化。生成期间可继续读取先前已发布的内容版本；内容任务失败不会撤销解析结果或阻断问答。超出首版12批覆盖预算的长字幕会显式标记为 partial。

接口：

- `GET /v1/results/{result_id}/content`：读取当前内容版本和状态。
- `POST /v1/results/{result_id}/content-jobs`：按请求键幂等创建新 revision。
- `GET /v1/content-jobs/{job_id}`：查询任务状态。
- `GET /v1/content-jobs/lookup?client_request_key=...`：按请求键恢复受理结果。
- `GET /v1/content-artifacts/{artifact_id}`：读取不可变概览/章节产物。

`000011_content.sql`已由API启动应用；M32以本地确定性fixture走通内容Worker、revision发布及API读取，M33加入profile全量快照/hash校验；真实字幕/百炼请求、结构化输出兼容性和内容质量仍未验证。首版运行一个独立内容 Worker 槽；M17已实现同workspace/model共享RPM/TPM门闩；M18提供API/Worker/provider基础指标；M19已接入Prometheus抓取、告警、pgxpool与解析/内容/删除/Outbox/ASR积压快照；M46增加Kafka consumer group lag、offset过期和lag采集失败指标/告警；M47把持久化Kafka拒收记录计数纳入队列快照；M48用隔离Prometheus实测数据库快照失败告警进入firing。跨workspace账户级额度及自动质量评估仍待后续里程碑完成。


## 删除视频

`DELETE /v1/videos/{id}`会立即把视频标记为`DELETING`、停止新解析/问答并返回删除任务；`GET /v1/video-deletions/{deletion_id}`读取进度及`blocking_reason`。后台删除任务先等待24小时安全窗口和未完成上传收敛，再删除已登记的OSS对象与数据库内容。已知task_id且不再由解析Worker消费的Fun-ASR远端任务由独立reconciler查询，供应商终态才解除等待；UNKNOWN且task_id不可得时保持等待并提示人工/供应商核实。查询404、超时或未知状态都不等于远端已终止。M72以隔离PostgreSQL与provider替身验证：视频删除后旧epoch迟到返回task ID，reconciler查到远端终态后才解除删除门禁。M73验证初始提交加一次自动补交的预算跨Worker epoch保存，第三次预留被拒绝。完成后只保留用于阻止旧请求重放的轻量墓碑。

删除迁移为`000012_video_deletion.sql`，`000013_video_gc_batches.sql`保存逐表清理阶段，`000014_video_gc_indexes.sql`增加按视频定位清理行的索引，`000015_video_gc_discovery.sql`增加持久化OSS扫描游标，`000016_asr_reconciliation.sql`增加远端ASR终态和对账租约，`000017_provider_quota.sql`增加百炼Embedding/Qwen共享配额门闩与调用预留账本。对象键按video_id前缀分页枚举源视频、音频和ASR结果的全部版本/删除标记，并在数据库清理前后各扫一次；发现对象写入现有删除账本后再逐个删除。每个事务最多处理500行，阶段推进与本批修改同事务提交；一次领取最多25批后释放租约，重启后从持久化阶段继续。新实现已加入OSS前缀扫描来发现未登记的对象版本；历史源视频键不含video_id，依赖持久化upload_session/blob记录覆盖。M71新增隔离PostgreSQL schema的删除Worker集成测试：实际执行初始和末轮三前缀扫描、登记并删除源对象及发现的孤儿对象、分阶段清理数据库并完成DELETED墓碑；测试对象存储为本地替身，且将任务调度时间提前以跳过24小时安全窗口。M72新增迟到Fun-ASR task ID对账集成测试，M73新增跨run_epoch的ASR补交预算测试，provider响应仍是本地替身。尚无真实OSS版本列表/删除、真实Fun-ASR任务对账、并发写入竞争或真实百炼模型验收。

## 开发

```sh
go test ./...
go vet ./...
```

PostgreSQL集成测试可在本地依赖启动后显式运行：

```sh
TEST_DATABASE_URL='postgres://mirrorcore:mirrorcore-local-only@localhost:5432/mirrorcore?sslmode=disable' go test ./... -count=1
```

该命令启用本地 PostgreSQL 集成用例；Kafka/PostgreSQL交接用例还要求设置`TEST_KAFKA_BROKERS`，并应连接独立测试broker，避免与正在运行的本地consumer group争抢分区。M74已用单独端口的临时Kafka broker和隔离PostgreSQL schema运行该用例；其余Fun-ASR、Embedding与Qwen的HTTP协议测试使用本地mock server，不调用云服务。上传完成恢复集成测试使用隔离schema和对象存储故障替身。

工作进度和真实联调缺口记录在 `docs/development-log.md`。

## Agent效果评估

离线评估器从JSONL读取相同初始检索下的固定RAG与Agent配对运行，并输出按split/类别分层的证据、回答和运行成本指标：

```sh
go run ./cmd/agent-eval -input assets/evaluation/synthetic_fixture.jsonl
```

仓库示例只验证评估器，不代表模型效果或真实费用。人工数据契约、配置清单和标注流程见 [`docs/agent-evaluation.md`](docs/agent-evaluation.md)。

先从冻结的问题计划运行固定 RAG / Agent 配对：

```sh
go run ./cmd/qa-eval-run -plan frozen-plan.jsonl -output paired-runs.jsonl
```

默认最多1题；更大批次需显式设置`-max-cases`（上限50）。在真实模式下，必须显式加`-allow-provider-calls`才会执行百炼请求；该命令可能产生费用。本地`LOCAL_DEMO_MODE=true`只能运行`synthetic_fixture`。运行输出可直接交给`qa-eval-export`生成标注草稿；导出器指定文件时拒绝覆盖已有草稿，保护人工审核内容。计划字段、稳定重放行为和费用保护见 [`docs/agent-evaluation.md`](docs/agent-evaluation.md)。

已有固定 RAG 与 Agent 问答记录可通过标注计划导出为人工审核草稿：

```sh
go run ./cmd/qa-eval-export -plan annotation-plan.jsonl -output annotation-draft.jsonl
```

命令只读取PostgreSQL，不调用模型；会校验同视频、同`result_id`、检索配置和初始上下文，并保留原始答案供人工盲审。`human_annotated`草稿必须先补充声明/引用判断并标记人工复核，之后才能交给`agent-eval`。字段和计划样例见 [`docs/agent-evaluation.md`](docs/agent-evaluation.md)。本地合成线路可用`go run ./cmd/qa-strategy-smoke -keep-questions`保留临时问题供导出；默认仍会清理，保留模式只用于本机fixture。

生成策略盲化的A/B审核包，并在审核完成后映射回原始评估草稿：

```sh
go run ./cmd/qa-eval-review pack -draft annotation-draft.jsonl -output-dir blind-review
go run ./cmd/qa-eval-review apply -draft annotation-draft.jsonl -pack blind-review/review.jsonl -key blind-review/answer-key.json -submission reviewer-01.jsonl -output reviewed.jsonl
```

只分享`review.jsonl`和`review-template.jsonl`；保留`answer-key.json`。审核工具不调用云服务，synthetic fixture不能通过导入成为人工质量数据。完整字段、保密边界和导入规则见 [`docs/agent-evaluation.md`](docs/agent-evaluation.md)。

在已完成的合成语音媒体 fixture 上运行固定 RAG 与 Agent 两条真实 Go 问答编排路径：

```sh
go run ./cmd/qa-strategy-smoke
```

需先按上文配置本地环境并运行 `seed-media-demo`，等待 task 成功。命令只使用 local-demo 确定性向量与答案 provider，会临时创建两条问题记录并在正常退出时删除；它先用不存在的`result_id`验证受理拒绝，再让两条问答都在视频行锁下要求同一个结果版本，并核对结果版本、初始检索排序及工具调用轨迹。它不用于评估语义质量、云端成本或生产时延。固定 RAG 通过内部评估构造器选择，公开问答 API 仍创建 Agent 策略。
