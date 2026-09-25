# Java Learning Project

一个长期演进的 Java 学习实验场：先用纯 Java 理解核心机制，再让同一个交易领域逐步吸收 Spring、数据库、中间件和分布式设计。

## 当前阶段

**Stage 1：Java 基础补强（进行中）**

当前代码只包含一个内存订单示例和一个并发实验。没有提前引入 Spring、数据库、缓存、消息队列或微服务。

## 学习路线

1. **Stage 1 — Java 基础：** OOP、异常、字符串、集合、泛型、Lambda、Stream、Optional、基础 IO。
2. **Stage 2 — Java Core：** 集合实现、泛型原理、IO/NIO、Buffer、Channel、Selector、序列化。
3. **Stage 3 — 并发与 JVM：** 线程、同步、CAS、锁、线程池、CompletableFuture、JVM 内存、类加载、GC、JIT、字节码和性能分析。
4. **Stage 4 — Spring：** IoC、DI、AOP、Bean 生命周期、MVC、Boot、校验、事务与 Actuator。
5. **Stage 5 — 数据与消息：** SQL、MySQL、事务、索引、MVCC、Redis、缓存和一种 MQ。
6. **Stage 6 — 分布式：** RPC、服务发现、幂等、重试、超时、限流、熔断、分布式 ID 与一致性。
7. **Stage 7 — 微服务与性能：** Gateway、注册/配置中心、集群、搜索、观测、Docker、压测与 JVM 调优。
8. **Stage 8 — 架构与源码：** 研究框架、中间件和 JVM 源码，将交易、用户、库存、支付、风控和通知逐步演进为综合系统。

各阶段的边界和引入条件见 [`docs/architecture/roadmap.md`](docs/architecture/roadmap.md)。每项重要结构决策记入 [`docs/architecture/adr/`](docs/architecture/adr/)。

## 技术栈演进

| 阶段 | 技术基线 |
| --- | --- |
| 1–3 | Java 17、Maven、JUnit 5；并发和 JVM 内容在需要时逐步增加 |
| 4 | 在理解 Java 基础后引入适配 Java 17 的 Spring Boot 稳定版本 |
| 5 | 先 MySQL，再 Redis，再选择一种 MQ；每项技术配套学习目标和 ADR |
| 6–8 | 按实际问题逐步引入分布式、微服务、可观测性、性能工具和源码研究 |

未来框架版本在进入对应阶段时再根据官方兼容矩阵确认，避免现在固定尚未使用的技术依赖。

## 当前学习目标

- [x] 建立 Java 17 Maven 单体项目和 JUnit 5 测试入口
- [x] 用纯 Java 建立最小商品与订单领域示例
- [x] 准备一个可独立运行的线程安全性实验
- [ ] 完成 OOP、异常、集合、泛型、Lambda/Stream、Optional 与基础 IO 的配套练习
- [ ] 随学习进度补齐边界、异常和参数化测试
- [ ] 总结每个实验的假设、观察和结论

## 当前目录结构

```text
.
├── .mvn/wrapper/                 # Maven Wrapper 配置
├── docs/
│   ├── ai/                       # AI 辅助学习规范、提示词和记录模板
│   ├── architecture/adr/         # 架构决策记录与演进路线
│   ├── database/                 # 数据持久化阶段的设计入口
│   ├── learning/lab/             # 学习目标、实验说明与结论
│   └── notes/                    # 可复用的学习笔记模板
├── src/main/java/com/example/learning/
│   ├── application/              # 可运行的学习入口
│   ├── domain/                   # 当前阶段实际使用的商品和订单模型
│   └── lab/concurrency/          # 可独立运行的并发实验
└── src/test/java/                # 与业务代码对应的 JUnit 5 测试
```

只为当前代码创建 package；新的学习主题在有真实实验内容时再加入。

## 如何运行

要求本机安装 **JDK 17**。先检查 `java -version`；Maven Wrapper 固定使用 Maven 3.9.16，但它不会替你安装 JDK。

Windows PowerShell：

```powershell
java -version
.\mvnw.cmd compile
java -cp target\classes com.example.learning.application.LearningApplication
```

macOS/Linux：

```bash
java -version
./mvnw compile
java -cp target/classes com.example.learning.application.LearningApplication
```

首次使用 Wrapper 需要联网下载 Maven 和 Maven 依赖。若不使用 Wrapper，也可用已安装且足够新的 Maven 执行同样的 Maven 目标。

## 如何运行测试

Windows PowerShell：

```powershell
.\mvnw.cmd test
```

macOS/Linux：

```bash
./mvnw test
```

当前订单测试覆盖正常金额计算、无效数量边界和商品不存在异常。测试已加入项目；运行时需要 JDK 17 和首次依赖下载所需的网络连接。

## 学习实验

- [并发：未同步计数器](docs/learning/lab/concurrency/README.md)：观察复合读改写操作的竞态。实验结果具有不确定性，不作为正确性测试。
- [AI 辅助学习规范](docs/ai/README.md)：先独立分析、实现，再请求审查和总结。

## 架构演进

初期保持单 Maven 项目、单体和内存数据。先让领域规则与学习实验清楚可测；Stage 4/5 再评估持久化端口与适配器，未来需要时才拆 Maven 模块或服务。演进约束见 [`docs/architecture/adr/ADR-001-initial-project-structure.md`](docs/architecture/adr/ADR-001-initial-project-structure.md)。

## 当前环境提示

搭建时检测到此工作区默认 `java` 为 1.8.0_162、`mvn` 为 3.3.9。项目目标是 Java 17；请将本机 `JAVA_HOME`/`PATH` 切换到 JDK 17 后再编译。Maven Wrapper 统一 Maven 版本，不会改变当前使用的 JDK。
