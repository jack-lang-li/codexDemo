# Java 企业后端学习实验项目

这是一个用于循序学习 Java 高级开发、Spring 企业后端、微服务和分布式系统的 Maven 实验项目。当前代码仍以 Java 基础、订单领域示例和并发实验为主；`pom.xml` 已为后续学习准备常见企业技术栈。依赖就位不代表相关中间件已经配置或启动，按学习阶段逐项接入，不要一次启用所有基础设施。

## 技术基线

| 类别 | 当前基线 |
| --- | --- |
| 语言与构建 | Java 17、Maven Wrapper 3.9.16 |
| Spring Boot | 3.5.16 |
| Spring Cloud | 2025.0.3 release train，适配 Spring Boot 3.5.x |
| Spring Cloud Alibaba | 2025.0.0.0，2025.0.x 线适配 Spring Cloud 2025.0.x / Spring Boot 3.5.x |
| ORM | MyBatis-Plus 3.5.17（Spring Boot 3 starter） |
| 数据库 | MySQL Connector/J、Druid Boot 3 starter、Flyway |
| 缓存 | Spring Data Redis、Redisson 4.7.0（显式选择 Spring Data Redis 3.5 适配模块） |
| 消息 | RocketMQ Spring 2.3.6、Spring for Apache Kafka、Spring AMQP / RabbitMQ |
| 搜索 | Spring Data Elasticsearch |
| 微服务 | Nacos、OpenFeign、Spring Cloud Gateway、Sentinel、Seata |
| 调度 | Quartz、XXL-JOB 3.4.2 |
| API 文档 | SpringDoc OpenAPI 2.9.1 / Swagger UI |
| 可观测性 | Spring Boot Actuator、Micrometer Prometheus、Logstash Logback Encoder 8.1 |
| 安全 | Spring Security、JJWT 0.13.0 |
| 通用工具 | Lombok、Apache Commons Lang3、Guava、Hutool |
| 文件存储 | MinIO Java SDK 9.0.3 |
| 测试 | Spring Boot Test（JUnit Jupiter、Mockito、AssertJ） |

Spring Boot 负责 Spring 生态常用依赖版本；Spring Cloud、Spring Cloud Alibaba、MyBatis-Plus 和 JJWT 通过 BOM 集中管理。Boot 3 使用 Druid 的 `druid-spring-boot-3-starter`。Redisson 4.x 默认选择 Spring Data Redis 4.x 适配器，本项目排除该模块并使用 `redisson-spring-data-35`，对应 Boot 3.5 的 Spring Data Redis 版本。

> 支持状态：因项目明确要求 Spring Boot 3.x，这里选用 3.5 线最后一个 OSS 版本 3.5.16。Spring 官方已说明它是 3.5.x 的最后一个 OSS release；若用于生产环境，需要企业支持或规划迁移到 Spring Boot 4.x。

**运行边界：** 这是单 Maven 模块的学习依赖集合，不是声称所有中间件已集成的生产服务。Spring Cloud Gateway 使用 WebFlux/reactive 技术栈，而本项目的普通 Web API 使用 Spring MVC；Gateway 应作为单独服务或独立模块学习，不要把两种 Web 栈当作一个运行入口。多个 MQ、数据库、缓存、搜索和调度组件也应按主题分开配置与运行。

## 学习顺序

1. **Java 基础与核心：** OOP、异常、集合、泛型、Lambda/Stream、Optional、IO/NIO、集合实现和常见设计模式。
2. **并发与 JVM：** 线程安全、锁、CAS、线程池、CompletableFuture、JVM 内存、类加载、GC 和性能分析。
3. **Spring 基础：** Spring IoC/DI、Bean 生命周期、配置、AOP、事务，以及 Spring Boot 自动配置和 Actuator。
4. **Web API：** Spring MVC、REST、参数校验、统一异常处理、SpringDoc 与 API 测试。
5. **安全：** Spring Security 认证/授权、密码处理、过滤器链、JWT 生命周期与密钥管理。
6. **关系数据库：** SQL 与 MySQL 事务、索引、锁、MVCC；再学习 MyBatis-Plus、连接池指标和 Flyway 迁移。
7. **缓存：** Redis 数据结构、缓存一致性和失效策略；然后用 Redisson 学分布式锁、限流和集合。
8. **消息驱动：** 先理解消息语义、重试、幂等和死信，再依次比较 Kafka、RabbitMQ、RocketMQ 的模型和适用场景。
9. **可观测性：** 结构化日志、日志脱敏与关联 ID、Actuator、Micrometer 指标和 Prometheus。
10. **微服务：** 先服务发现和配置中心（Nacos），再 OpenFeign、Gateway、Sentinel；最后通过故障场景学习 Seata 与分布式事务边界。
11. **任务调度：** 先学习单应用 Quartz，再学习 XXL-JOB 的调度中心、执行器、分片和故障恢复。
12. **搜索与对象存储：** 学 Elasticsearch 的索引/查询与数据同步，再学 MinIO 的桶、对象、权限和预签名 URL。
13. **工具库与综合实践：** 仅在标准库不能清楚解决问题时引入 Commons Lang、Guava、Hutool 或 Lombok；逐步形成有监控、迁移、重试和恢复方案的综合练习。

每个主题先写学习问题和预期，再实现最小实验，记录观察、失败方式和设计取舍。每个中间件的独立目录见 [`docs/learning/stack/README.md`](docs/learning/stack/README.md)。

## 当前代码与文档

```text
src/main/java/com/example/learning/   # 当前 Java 示例、领域对象和并发实验
src/test/java/                         # 单元测试与待整理的学习示例
docs/learning/stack/                   # 中间件与基础组件的一主题一目录
docs/learning/lab/                     # 可运行实验说明和观察记录
docs/architecture/                     # 演进路线与架构决策记录
docs/database/                         # 数据持久化学习入口
docs/notes/                            # 学习笔记模板
docs/ai/                               # AI 辅助学习规范与模板
```

当前实验：[未同步计数器](docs/learning/lab/concurrency/README.md)。架构演进约束见 [`docs/architecture/roadmap.md`](docs/architecture/roadmap.md)，数据库学习入口见 [`docs/database/README.md`](docs/database/README.md)。

## 运行

要求 JDK 17 或更高版本。Maven Wrapper 固定 Maven 3.9.16，但不会替你安装或切换 JDK。

Windows PowerShell：

```powershell
java -version
.\mvnw.cmd compile
```

macOS/Linux：

```bash
java -version
./mvnw compile
```

首次使用 Wrapper 需要联网下载 Maven 和项目依赖。完成环境准备后可用 `.\mvnw.cmd test`（Windows）或 `./mvnw test`（macOS/Linux）运行测试。

## 版本兼容依据

- [Spring Cloud 版本映射与 BOM 指南](https://spring.io/projects/spring-cloud)：2025.0.x 对应 Spring Boot 3.5.x。
- [Spring Cloud Alibaba 2025.x 版本说明](https://sca.aliyun.com/en/docs/2025.x/overview/version-explain/)：2025.0.0.0 对应 Spring Cloud 2025.0.0 / Spring Boot 3.5.0。
- [MyBatis-Plus 安装说明](https://baomidou.com/en/getting-started/install/)：Spring Boot 3 使用 `mybatis-plus-spring-boot3-starter`。
- [Redisson Spring 集成说明](https://redisson.pro/docs/integration-with-spring/)：适配器后缀需匹配 Spring Data Redis 版本。
