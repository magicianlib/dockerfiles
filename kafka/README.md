## KRaft 版本说明

Kafka 支持 KRaft（KIP-500）模式的最小版本取决于具体的使用场景：

**最早引入版本（体验/预览版）：Kafka 2.8.0**

说明：Kafka 在 2.8.0 版本首次引入了 KRaft 机制（Early Access），可以脱离 ZooKeeper 运行，但功能不完整（如缺乏完整的 ACL、SCRAM、动态配置等支持），仅适用于测试与评估。

**生产可用最小版本（Production Ready）：Kafka 3.3.0**

说明：在 Kafka 3.3.0 中，KRaft 正式宣布达到生产环境可用标准，补齐了元数据高可用、故障恢复、集群平滑升级等关键特性。

**成为唯一架构的版本：Kafka 4.0+**

说明：Kafka 4.0 彻底废弃并移除了 ZooKeeper 模式，KRaft 成为 Kafka 的唯一且强制的元数据管理架构。

## 监听器配置说明

Kafka 的网络由两个配置共同决定，容易混淆：

| 配置 | 含义 | 该填什么 |
|------|------|---------|
| KAFKA_LISTENERS | broker 监听（绑定）的地址，即“在哪听” | 用 `0.0.0.0`，监听容器内所有网卡 |
| KAFKA_ADVERTISED_LISTENERS | broker 对外宣告给客户端的地址，即“告诉别人怎么连我” | 必须是客户端能真实访问到的地址，不能用 `0.0.0.0` |

客户端先连上 broker（监听在 `0.0.0.0` 所以能连上），broker 把宣告地址回给客户端，客户端再改用这个地址继续通信。因此宣告地址必须可达；填 `0.0.0.0` 会让客户端无处可连，而且 Kafka 启动时会直接报错拒绝。

本仓库的宣告地址分两段：

- **PLAINTEXT**（容器之间内部通信）：用容器名，如 `kafka:19092`，靠 Docker 内部 DNS 解析，固定不变。
- **EXTERNAL**（宿主机或外部客户端接入）：用 `${ADVERTISED_HOST:-localhost}`，由环境变量控制。

### ADVERTISED_HOST 变量用法

EXTERNAL 的对外地址由 `ADVERTISED_HOST` 控制，语法 `${ADVERTISED_HOST:-localhost}` 表示“未设置时回退到 localhost”：

- 本机开发（默认）：直接 `docker compose up -d`，对外地址就是 localhost。
- 远程服务器部署，二选一：
  - 启动时临时指定：`ADVERTISED_HOST=你的服务器IP docker compose up -d`
  - 或在对应目录放一个 `.env` 文件（docker compose 会自动读取），写入 `ADVERTISED_HOST=你的服务器IP`，之后直接 `docker compose up -d`

PLAINTEXT 段不经过这个变量，始终用容器名，无需改动。

## CDC 对源数据库的要求

Debezium 通过读取源数据库的变更日志实现 CDC。下面以常用的 PostgreSQL、MySQL 为例说明源库需要满足的条件。

### PostgreSQL（本仓库 postgresql17 已开启逻辑复制）

服务器级参数（在 `postgresql.conf` 或容器启动参数设置，改完需重启）：

| 参数 | 要求 | 说明 |
|------|------|------|
| `wal_level` | `logical` | 开启逻辑解码 |
| `max_replication_slots` | ≥1（本仓库设 10） | 复制槽数量，按 Debezium 连接器数量调整 |
| `max_wal_senders` | ≥1（本仓库设 10） | WAL 发送进程数，按连接数调整 |

其他要点：

- **逻辑解码插件**：Debezium 2.x 默认用 PostgreSQL 内置的 `pgoutput`（PG10+ 自带，无需额外安装）。
- **复制槽与 publication**：由 Debezium 自动创建，无需手动建。
- **REPLICA IDENTITY**：默认 `DEFAULT`；若要捕获 UPDATE/DELETE 的完整旧值或无主键表的删除，需在表级设为 `FULL`。
- **版本**：Debezium 2.7 支持 PostgreSQL 12~17。

账号权限：需 `REPLICATION` 角色 + `LOGIN` + 对库 `CREATE` + 对捕获表 `SELECT`。示例：

```sql
-- 创建用于 CDC 的账号，必须具备逻辑复制权限（REPLICATION 角色）
CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD '你的密码';
-- 允许连接到目标数据库（把“你的库”替换为实际库名）
GRANT CONNECT ON DATABASE 你的库 TO debezium;
-- 授予 schema 使用权限；public 仅为示例，请替换为表所在的 schema，
-- 表分布在多个 schema 时需对每个 schema 分别授权
GRANT USAGE ON SCHEMA public TO debezium;
-- 授予现有表的读取权限（schema 同样按实际替换）
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
-- （可选）让后续新建表自动获得读取权限，避免每次手动授权
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT TO debezium;
```

**wal_level 值说明**

|wal_level 值|包含的信息|适用场景|
|:-------------|:-------|:------|
|minimal|仅包含崩溃恢复所需的最小日志（不记录部分批量操作的日志）|单机运行、极高并发批量写入场景（不支持任何主从复制）|
|replica（默认）|包含崩溃恢复、WAL 归档以及物理流复制（Hot Standby）所需的信息|搭建主从高可用集群、只读副本、物理灾备|
|logical|包含 replica 的所有信息，外加逻辑解码信息（表结构元数据、行级变更记录等）|逻辑复制（Pub/Sub）、跨大版本平滑升级、CDC 实时数据入仓|

### MySQL

| 参数 | 要求 | 说明 |
|------|------|------|
| `log_bin` | 开启 | 启用 binlog |
| `binlog_format` | `ROW` | 必须，STATEMENT/MIXED 不支持 |
| `binlog_row_image` | `FULL` | 保留完整前后镜像 |
| `server_id` | 唯一正整数 | 标识实例 |
| `gtid_mode` | `ON`（推荐） | 便于断点续传 |

账号权限：`REPLICATION SLAVE` + `REPLICATION CLIENT` + 对库 `SELECT`。示例：

```sql
CREATE USER 'debezium'@'%' IDENTIFIED BY '你的密码';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
```

通用提示：Kafka Connect 容器需能网络访问源库；账号建议最小权限，避免直接用超级用户。

## 参考来源

- [Apache Kafka Quickstart](https://kafka.apache.org/quickstart/)：KRaft 模式与官方镜像的基础用法。
- [Running Apache Kafka KRaft on Docker（Instaclustr）](https://www.instaclustr.com/education/apache-spark/running-apache-kafka-kraft-on-docker-tutorial-and-best-practices/)：多节点 KRaft 集群的集群标识生成与部署最佳实践。
- [Docker Forums：apache/kafka 默认 CLUSTER_ID 行为](https://forums.docker.com/t/kafka-fails-to-start/147141)：未显式设置集群标识时镜像使用的默认值与日志特征。
- [Confluent Docker 配置参考](https://docs.confluent.io/platform/current/installation/docker/config-reference.html)：集群标识环境变量（CLUSTER_ID）的命名约定说明。
- [Debezium: CDC](https://debezium.io/documentation/)：Debezium 官方文档。
- [Debezium: PostgreSQL 连接器](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)：源库配置与权限要求。
- [Debezium: MySQL 连接器](https://debezium.io/documentation/reference/stable/connectors/mysql.html)：binlog 与权限要求。