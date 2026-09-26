# 数据持久化学习入口

项目 POM 已预先声明 MySQL Connector/J、MyBatis-Plus、Druid 和 Flyway，作为后续实验的依赖基线；当前订单示例仍使用内存中的不可变 Map，不代表数据库连接、迁移或 Repository 已经实现。按学习主题逐项接入。

先围绕当前订单示例写下要回答的问题：

- 进程重启后，订单和商品数据如何保留？
- 订单创建与库存扣减需要哪些事务边界？
- 需要哪些查询、索引和约束？
- 哪些规则属于领域对象，哪些属于存储适配器？
- 是否需要 Repository 接口；选择 JDBC、MyBatis 或其他方式的理由是什么？

建议先用一份 ADR 比较方案，再用 Flyway 管理 schema，接着分别学习 MySQL、MyBatis-Plus、连接池观测。Redis 和各 MQ 保持为独立实验，不打包成一次升级。

专题目录：

- [MySQL](../learning/stack/mysql/README.md)
- [MyBatis-Plus](../learning/stack/mybatis-plus/README.md)
- [Druid](../learning/stack/druid/README.md)
- [Flyway](../learning/stack/flyway/README.md)
