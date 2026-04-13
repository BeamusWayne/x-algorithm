# X For You Feed · 系统综述

## 一句话概括

用户打开 X，系统从两路来源召回帖子，经过过滤、ML 评分、排序后，返回个性化信息流。整个过程无手工特征工程，全靠 Grok Transformer 从互动历史中自动学习相关性。

---

## 系统组成

| 模块 | 语言 | 职责 |
|------|------|------|
| `home-mixer` | Rust | 流水线编排层，对外暴露 gRPC 接口 |
| `thunder` | Rust | 内存帖子引擎，实时消费 Kafka 事件 |
| `phoenix` | Python/JAX | ML 核心：两阶段召回 + Transformer 排序 |
| `candidate-pipeline` | Rust | 可复用流水线框架（Source/Filter/Scorer…） |

---

## 请求全链路

```
用户请求
   │
   ▼
Home Mixer（编排层）
   │
   ├─ [1] Query Hydration     获取用户互动历史 + 关注列表
   │
   ├─ [2] Candidate Sourcing  并行召回候选
   │        ├─ Thunder         → 已关注账号的近期帖子（站内，亚毫秒级）
   │        └─ Phoenix 检索    → ML 相似度搜索全量语料库（站外）
   │
   ├─ [3] Hydration           补全帖子元数据（文本/媒体/作者/订阅状态）
   │
   ├─ [4] Pre-Scoring Filters 前置过滤（去重/过期/屏蔽词/已看过/付费墙…）
   │
   ├─ [5] Scoring             ML 多维评分
   │        ├─ Phoenix Scorer        Transformer 预测 15 种互动概率
   │        ├─ Weighted Scorer       加权合并 → 单一分数
   │        ├─ Author Diversity      同作者分数衰减，保证多样性
   │        └─ OON Scorer            站外内容分数调整
   │
   ├─ [6] Selection           按分数降序，取 Top-K
   │
   └─ [7] Post-Selection      后置过滤（删帖/垃圾/暴力 + 对话去重）
   │
   ▼
返回排序后的信息流
```

---

## Phoenix：两阶段 ML

### 阶段一：召回（Two-Tower）

```
用户特征 + 互动历史  ──▶  用户塔（Transformer）  ──▶  用户 Embedding
全量帖子语料库       ──▶  候选塔                  ──▶  帖子 Embedding

相似度搜索（点积 ANN）：百万候选 → Top-K 候选
```

### 阶段二：排序（Transformer + Candidate Isolation）

```
输入：[用户 Embedding | 历史序列 S 个 | 候选帖子 C 个]
         │                  │                  │
         └──────────────────┴──────────────────┘
                         Transformer
                    （特殊注意力掩码）
                         │
输出：[B, C, 15]  ←─ 每个候选 × 15 种行为概率
```

**注意力掩码规则（候选隔离）：**

|          | User | History | Candidates |
|----------|:----:|:-------:|:----------:|
| User     |  ✓   |    ✓    |     ✗      |
| History  |  ✓   |    ✓    |     ✗      |
| Candidate|  ✓   |    ✓    | 仅自身 ✓   |

候选之间完全隔离 → 评分独立、可缓存、不受批次影响。

---

## 评分公式

```
最终分数 = Σ (weightᵢ × P(行为ᵢ))

正权重：点赞 · 回复 · 转发 · 引用 · 点击 · 分享 · 关注作者 …
负权重：举报 · 拉黑 · 静音 · 不感兴趣 …
```

---

## 过滤器清单

**前置过滤（评分前运行，减少推断开销）**

| 过滤器 | 作用 |
|--------|------|
| DropDuplicatesFilter | 去除重复帖子 |
| AgeFilter | 过滤超时限旧帖 |
| SelfpostFilter | 不推荐自己的帖子 |
| MutedKeywordFilter | 过滤静音关键词 |
| AuthorSocialgraphFilter | 过滤被屏蔽/静音作者 |
| PreviouslySeenPostsFilter | 过滤用户已看过的内容 |
| PreviouslyServedPostsFilter | 过滤本次会话已下发内容 |
| IneligibleSubscriptionFilter | 过滤无权访问的付费内容 |
| RepostDeduplicationFilter | 同内容转发去重 |
| CoreDataHydrationFilter | 数据缺失则丢弃 |

**后置过滤（安全兜底）**

| 过滤器 | 作用 |
|--------|------|
| VFFilter | 删帖/垃圾/暴力/血腥等可见性过滤 |
| DedupConversationFilter | 同一对话多分支去重 |

---

## 五个核心设计决策

1. **零手工特征工程** — Transformer 直接从互动历史学习，无人工设计内容相关特征，大幅简化数据管道。

2. **候选隔离排序** — 候选间注意力屏蔽，分数与批次无关，可安全缓存复用。

3. **哈希 Embedding** — 用多哈希函数做向量查找，无需维护大型词表，天然支持新用户/新帖子。

4. **多行为同时预测** — 单次推断输出 15 种互动概率，调权重无需重训模型。

5. **可组合流水线** — `candidate-pipeline` 框架解耦执行与业务逻辑，各阶段独立可测、可并行。

---

## Thunder：实时内存存储

```
Kafka（帖子创建/删除事件）
        │
        ▼
   TweetEventsListener
        │
        ▼
   PostStore（内存）
   ├─ original_posts   原创帖子
   ├─ replies          回复与转发
   └─ video_posts      视频帖子

过期帖子自动清理 → 亚毫秒级查询，无需访问外部数据库
```

---

## 快速运行

```bash
# Phoenix ML 模型（Python）
cd phoenix
uv run run_ranker.py       # 运行 Transformer 排序器
uv run run_retrieval.py    # 运行 Two-Tower 召回
uv run pytest              # 运行测试

# Rust 服务
cargo run -p home-mixer    # 启动编排层 gRPC 服务
cargo run -p thunder       # 启动内存帖子引擎
cargo test                 # 运行所有 Rust 测试
```

---

*Licensed under Apache 2.0 · Transformer 架构移植自 [xAI Grok-1](https://github.com/xai-org/grok-1)*
