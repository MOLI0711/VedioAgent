# Agent 效果评估

## 目标与边界

评估在相同初始检索结果上比较固定 RAG 与 Agent 补查。它用于回答“补查是否补到必要证据、是否改善有证据支持的回答、增加多少调用与开销”，不能只看模型自评或向量相似度。

首版评估器只读取离线 JSONL，不调用模型或供应商。仓库中的 `synthetic_fixture` 仅验证数据契约与指标计算；它不是模型、检索、成本或性能结果，而且不能声明人工复核。只有人工标注且人工复核的 `human_annotated` 数据才会产生答案质量指标。

## 运行

```sh
go run ./cmd/agent-eval -input assets/evaluation/synthetic_fixture.jsonl
```

真实数据评估时，将输入路径换成审核后的 JSONL 文件。报告默认输出到标准输出；可用 `-output report.json` 保存。程序对未知字段、重复问题、错误状态、重复字幕句序号、无效引用、不同初检结果、跨视频划分泄漏等情况直接报错，不生成部分报告。

项目内还提供`cmd/qa-strategy-smoke`运行时线路检查。它在已完成的local-demo媒体fixture上分别实例化固定RAG与Agent服务，实际调用相同Embedding检索和数据库问答持久化，比较两次的初始排序，并从`question_steps.metadata`读取工具调用及新增证据句序号。两个问题均通过数据库视频行锁下的`RequiredResultID`受理前置条件固定到同一结果；如果active result中途变化，后一个请求会失败而不混比。该命令还会用一个错误结果ID确认受理返回`ErrQuestionResultChanged`。首轮检索记录原始排名和模型收到的初始上下文句序号；每次工具步骤记录新登记的句序号。该命令创建并清理临时问题，输出`dataset_kind=synthetic_fixture`；确定性答案不具语义评估价值。

## 从已运行问答导出标注草稿

`cmd/qa-eval-export`读取人工预先设计的评估计划和两个已终态问题ID，从数据库拼出一行JSONL草稿。它不运行模型、不调用供应商，也不替人判断答案是否正确。计划中应先填写问题类别、预期支持状态、必需回答点和可接受证据组，再运行固定RAG与Agent，防止看到模型输出后反向改写标准。

先用`cmd/qa-eval-run`从冻结计划执行成对问题。每行包含`schema_version=1`、`dataset_kind`、实验ID及共有配置指纹、稳定`run_id`、case/video/split、视频UUID与目标结果UUID、固定问题文本、类别、预期状态、必需回答点描述和证据组。`required_points`必须逐项对应`required_point_ids`，供审核者理解每个评分点。计划还可包含`fixed_rag_run_id`和`agent_run_id`；缺省时两侧使用`run_id`，一侧终态失败后可只更换失败侧的ID。执行成功后命令补入两个question ID，生成的同一计划可交给导出器。例如：

```json
{"schema_version":1,"dataset_kind":"human_annotated","experiment_id":"qa-v1","common_config_fingerprint":"sha256-of-frozen-common-manifest","run_id":"qa-v1-attempt-1","case_id":"course-a-001","video_key":"course-a","video_id":"00000000-0000-4000-8000-000000000010","target_result_id":"00000000-0000-4000-8000-000000000011","question_text":"视频中提出了哪两项改进？","split":"dev","category":"direct","expected_status":"answered","required_point_ids":["p1"],"required_points":[{"id":"p1","description":"说出讲者提出的第一项改进。"}],"required_evidence_groups":[{"id":"g1","sentence_seqs":[12,13]}]}
```

成对执行命令：

```sh
go run ./cmd/qa-eval-run -plan frozen-plan.jsonl -output paired-runs.jsonl
```

默认只执行1题；更大批次需显式提高`-max-cases`（上限50）。非`LOCAL_DEMO_MODE`时命令拒绝运行，除非显式传入`-allow-provider-calls`；这会真实调用百炼Embedding/Qwen并可能计费。每个case运行fixed RAG和Agent两条问答，受理时都用视频行锁要求计划中的同一`target_result_id`，并在输出前核对初始排名与初始上下文。命令使用数据库RPM/TPM limiter。输出文件以0600权限创建且拒绝覆盖，按case完成后立即flush；稳定幂等键覆盖实验、run、问题及冻结标注，重跑成功侧会复用已有问答。

若一侧已终态失败或中断，命令不会用同一键重复生成；更新该侧专用run ID可只重跑这一侧，保留另一侧成功结果。进程中断后，已写入输出文件的case可直接使用；未写出的成功问答会通过稳定request key复用。运行计划和共有配置指纹仍由实验者负责冻结；代码不能证明人工填写的指纹确实对应部署配置。

每条问答执行期间，CLI每10秒续一次数据库45秒租约，并把单题deadline作为执行context上限；续约失败会取消当前运行。若服务报告execution lease已丢失，CLI不会再尝试用旧execution ID写失败终态，避免过期执行者干扰接管后的状态。失败侧需使用新的策略级run ID重试。

运行方式：

```sh
go run ./cmd/qa-eval-export -plan annotation-plan.jsonl -output annotation-draft.jsonl
```

命令从`DATABASE_URL`连接PostgreSQL。每一对必须是同一视频、同一个固定`result_id`、同一检索profile、相同初始排序及相同实际初始上下文；问题策略按持久化`generation_profile`核实。计划中的video/result/question文本如果提供，也会和数据库记录逐项核对。含未完成question step的记录拒绝导出，避免把未落账的远端尝试当成零。`strategy_config_id`由实际持久化generation/retrieval profile规范化后计算SHA-256；`common_config_fingerprint`仍由实验者按冻结清单提供，工具无法证明清单与线上部署一致。

默认输出到标准输出；指定`-output`时以0600权限创建且拒绝覆盖已有文件，避免重新导出时抹掉已经人工编辑的草稿。若要重新生成，请使用新的文件名并自行核对版本。

草稿包含问题文本、问题/结果ID、必要回答点描述、必需证据原文与时间、初始排名、实际交给模型的初始证据、最终排名、最终提供证据及`raw_answer_json`。区分“候选排名”和“实际提供上下文”是为了避免把被字节预算裁掉的候选误当成模型看到的引用依据。`human_reviewed`默认为false，`claims`为空。人工可以直接标注；更建议使用下方匿名A/B流程。对`human_annotated`草稿，成功答案须逐条标注声明正确性、证据支持性和引用支持性，再正式评估。成本字段保持null，因为当前问答trace没有供应商账单事实；只有持久化记录到的token usage才导出。确定性local-demo中的usage数值不能当成模型消耗。

## A/B匿名盲审

`cmd/qa-eval-review pack`把导出的草稿转换为审核材料与私有答案键。每题使用随机`review_id`，A/B顺序和题目顺序分别随机；审核包只展示问题、必要回答点、支持证据原文/时间、两个候选回答及匿名引用。它不含策略名、question/result ID、数据划分、工具次数、token、费用或耗时。候选输出自身可能暴露风格特征，所以这降低直接期待偏差，但不保证完全不可推断。

```sh
go run ./cmd/qa-eval-review pack -draft annotation-draft.jsonl -output-dir blind-review
```

`blind-review`目录权限为0700，里面的文件为0600。只把`review.jsonl`与`review-template.jsonl`交给审核者；`answer-key.json`包含A/B与原策略、原question ID及文件SHA-256，必须留在研究者侧。审核者复制模板，为每题填写同一个`reviewer_id`，把A/B的`reviewed`设为true，并在`claims`里将模型回答拆成声明，标注`answer_block_id`、对应`point_id`、正确性、证据支持性和引用支持性。引用使用审核包中的匿名`evidence_ref`（如`S1`）；评分点ID也匿名为`P1`。

提交行示例：

```json
{"schema_version":1,"review_id":"opaque-review-uuid","reviewer_id":"annotator-01","candidates":[{"label":"A","reviewed":true,"claims":[{"answer_block_id":"B1","point_id":"P1","correct":true,"supported_by_evidence":true,"citations":[{"evidence_ref":"S1","supports_claim":true}]}]},{"label":"B","reviewed":true,"claims":[]}]}
```

若候选执行失败，也要把其`reviewed`设为true并保持`claims`为空。成功答案有回答块时，每块至少对应一条声明；同一回答块含多个独立事实时，可为该`answer_block_id`提交多条声明。没有回答块的成功结果（如明确材料不足）可提交空`claims`。

审核完后导入：

```sh
go run ./cmd/qa-eval-review apply \
  -draft annotation-draft.jsonl \
  -pack blind-review/review.jsonl \
  -key blind-review/answer-key.json \
  -submission reviewer-01.jsonl \
  -output reviewed.jsonl

go run ./cmd/agent-eval -input reviewed.jsonl
```

导入会核对原草稿与原审核包SHA-256、匿名case和A/B映射、答案块与引用范围，并要求本轮每个成功答案都已审核；结果会映射回原策略和字幕句序号，再通过正式评估校验。输出路径已存在时拒绝覆盖。每次导入只接受一个审核者；多审核者需分别导出reviewed文件并人工处理分歧，当前没有共识/仲裁聚合器。synthetic_fixture可以生成带显式警示的包用于文件链路演练，但导入器拒绝将其转换为人工质量数据。审核者身份是自填标签，不是身份认证。

本地线路冒烟可加`go run ./cmd/qa-strategy-smoke -keep-questions`保留两条临时问题，以便用plan导出；该模式仅用于本机合成fixture，默认不保留。导出后应按输出中的question_id删除这两条演示记录。正式数据集应使用预先标注、可合法使用的视频以及真实provider运行，并保留冻结的实验manifest。

## 数据契约

每行是一个问题，包含：

- `dataset_kind`：`synthetic_fixture` 或 `human_annotated`，一个文件内必须一致。
- `experiment_id`、`common_config_fingerprint`：固定本次实验和两种策略共享的配置。整个文件必须一致；指纹应覆盖模型及快照、Embedding/索引/转写profile、共有提示与答案契约、生成参数和代码提交。外部实验清单保存指纹对应的完整配置。
- `case_id`、`video_key`、`split`：问题、来源视频和 `dev`/`holdout` 划分。相同 `video_key` 不可出现在两个集合。
- `category`：`direct`、`multi_span`、`partial_support`、`insufficient`。
- `expected_status`：`answered`、`partial`、`insufficient`。
- `required_point_ids`：人工认可的必要回答点。
- `required_points`：可选的逐点文字说明；盲审`human_annotated`计划必须为每个required_point_id提供说明。
- `required_evidence_groups`：每个组代表一个必要证据单元；组内句序号是可替代命中项。跨段题用多个组，只有覆盖每个组才算完整覆盖。
- `question_text`、`evidence_sentences`：Exporter从冻结问题和固定结果转写中保留的问题文本与必需证据原文/时间戳，用于可独立阅读的盲审包。
- `runs`：必须是同一问题的 `fixed_rag` 与 `agent` 成对记录，且初始排序句序号完全相同。Agent 最终证据列表必须保留初始结果作为前缀，以便辨认补查增量。

每条run记录稳定的`strategy_config_id`、执行状态、预测支持状态、初始和最终检索排序、工具调用与无进展调用数、预算是否耗尽、HTTP尝试数、可选的输入/输出token与微美元成本、延迟。同一策略在文件中的配置ID必须一致；实验清单将该ID映射到固定/Agent提示词、工具开关与预算。`human_annotated`中成功的两种策略输出都必须由人工复核；每个声明关联`answer_block_id`，并标注正确性、证据支持性及逐条引用是否支持该声明。引用句必须出现在该run实际提供给模型的最终证据中。

策略对比应使用同一共有配置指纹、同一问题、同一初始检索排序；实验清单须确认模型快照、Embedding与索引版本、共有提示/输出契约、代码提交、温度和其他生成参数相同，策略配置只表达固定RAG与Agent工具策略差异。JSONL校验可验证配置ID在文件中稳定、初检完全相同，但不能证明指纹内容真实或两套部署确实使用了记录的配置，因此运行者必须保留生成清单与请求日志。

## 指标解释

| 指标 | 定义与限制 |
| --- | --- |
| completion rate | 成功终态数 / 全部尝试；失败与中断保留在分母。 |
| expected status accuracy | 成功回答中，answered/partial/insufficient 与人工预期一致的比例。 |
| 证据组覆盖率 | 每题命中的必需证据组数 / 该题全部必需组，再按问题宏平均。分别报告初检和最终结果。 |
| 完整证据组覆盖率 | 一题所有必需组都命中的问题比例。 |
| group MRR | 每个必需组取首个相关句排名的倒数，未命中记0，再按组和问题宏平均。 |
| correct point coverage | 人工判断正确的必要要点覆盖数 / 必要要点数。 |
| grounded point coverage | 同时正确且有支持证据的必要要点覆盖数 / 必要要点数。 |
| claim precision | 人工判断正确的声明数 / 已复核声明数。 |
| citation support precision | 被人工判定实际支持声明的引用数 / 已复核引用数。它不等同于完整引用召回率。 |
| unsupported claim rate | 无支持证据的声明数 / 已复核声明数。 |
| Agent配对差值 | 同题 Agent 减固定 RAG；正的质量差值通常更好，正的延迟、费用或工具调用差值表示代价增加。 |
| P95 latency | 最近秩法 `ceil(0.95*N)`；结果单独输出`sample_count`和rank，小样本 P95 不稳定。 |

无分母的指标以 `value: null` 表示，不当作0。报告按split与问题类别分层，包含每层视频数；逐题Agent配对差值带case_id，避免总均值掩盖材料不足题或跨段题表现。报告仍需结合外部实验清单解释配置指纹。

## 人工数据构建流程

1. 选取5段可合法使用的视频或讲解材料，覆盖短内容、多主题、术语/口音、中英混合及无语音场景；同一视频派生的问题只能落在同一个 split。
2. 建立50题计划：20道直接定位、10道跨段综合、10道部分支持、10道材料不足。每题由标注者先依据视频/转写定义问题、必要知识点、支持证据组、允许状态及答案要点。
3. 固定转写与任务版本。记录人工认为的真实含义以及当前字幕可支持的事实，区分 ASR 错误与检索/回答错误。
4. 在冻结问题和标注之后，用完全相同的初检候选分别运行固定 RAG 与 Agent。记录实际输入输出用量、成本、调用次数和时间，不从估算值补账。
5. 对两组输出进行盲审：逐条标注答案声明正确性、证据支持性与引用支持性。存在分歧时复核并记录最终决定。
6. 先用 dev 集调整；配置和判定规则冻结后只运行一次 holdout 对比。若根据 holdout 错误修改 profile，该 holdout 即转为开发材料，需新增未见视频形成新保留集。

50题只是发现问题的初始样本，不足以证明生产泛化。先报告计数、类别分层与具体失败案例；样本不足时不做强结论。

## 当前状态

- 评估计算器、严格 JSONL 校验和示例夹具已落地。
- M60增加已存问答轨迹导出：人工标注计划→数据库中的成对question→annotation-draft JSONL；未审核的人类数据不能被正式评估器接收。
- M61增加冻结计划的配对执行器：固定`result_id`创建fixed RAG/Agent问题，成功后输出question ID；默认单case上限，真实百炼调用需显式打开。
- M63增加离线A/B匿名盲审包、独立私有答案键、带匿名评分点/引用模板及严格映射导入；文件链路只以synthetic_fixture演练，没有真实人工标签或模型质量结论。
- fixed RAG 与 Agent 共用问答服务的固定版本检索、证据登记、答案Schema校验和落库路径；策略保存在generation profile中，固定RAG评估构造器不扩展公开API。
- 尚无真实人工标注集，没有真实 Qwen、Embedding、Fun-ASR 调用的质量或成本数据。
- 尚未通过评估证明 Agent 补查胜过固定 RAG。
- 未达到数据计划前，不开展模型 SFT/RL；先建立可靠的行为基线和可审核轨迹。
