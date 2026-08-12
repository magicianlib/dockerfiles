Kafka 支持 KRaft（KIP-500）模式的最小版本取决于具体的使用场景：

**最早引入版本（体验/预览版）：Kafka 2.8.0**

说明：Kafka 在 2.8.0 版本首次引入了 KRaft 机制（Early Access），可以脱离 ZooKeeper 运行，但功能不完整（如缺乏完整的 ACL、SCRAM、动态配置等支持），仅适用于测试与评估。

**生产可用最小版本（Production Ready）：Kafka 3.3.0**

说明：在 Kafka 3.3.0 中，KRaft 正式宣布达到生产环境可用标准，补齐了元数据高可用、故障恢复、集群平滑升级等关键特性。

**成为唯一架构的版本：Kafka 4.0+**

说明：Kafka 4.0 彻底废弃并移除了 ZooKeeper 模式，KRaft 成为 Kafka 的唯一且强制的元数据管理架构。