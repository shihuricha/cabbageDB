# CabbageDB

本项目是一个轻量级分布式 SQL 数据库（Go），专为学习数据库核心技术设计，核心模块自主实现，支持分布式容错、ACID 事务及标准 SQL 查询。

### **核心模块与功能**

#### **1. 存储引擎**

- **Bitcask 日志合并存储**：
  - 数据以仅追加（append-only）方式持久化到磁盘，支持快速写入。
  - 启动时自动合并压缩无效数据（如删除标记），减少磁盘占用。
  - 内存中维护 BTreeMap 索引，加速主键查询与范围扫描。

#### **2. SQL 语法解析**

- **解析器**：
  - **手写词法分析器（Lexer）**：逐字符解析 SQL 字符串，生成 Token 流（如识别关键字、标识符、运算符等）。
  - **基于 Goyacc 的语法分析器**：通过自定义文法规则（Yacc 规范），将 Token 流转换为抽象语法树（AST）。

#### **3. SQL 查询与执行**

[具体SQL语法](./doc/sql.md)

- **SQL 兼容性**：
  - 支持标准 DDL/DML（`CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, `SELECT`）。
  - 提供聚合函数（`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`）、多表连接（`INNER JOIN` `OUTER JOIN`）及排序分页（`ORDER BY`, `LIMIT`）。
- **查询优化**：
  - 自动优化规则：谓词下推、常量折叠、索引查找优化、冗余节点清理、join连接优化。
- **物理执行**：
  - 基础算子：索引扫描（`IndexScan`）、过滤（`Filter`）、哈希连接（`HashJoin`）、聚合（`Aggregate`）。

#### **4. 分布式架构**

- **Raft 多副本共识**：
  - 手动实现 Raft 协议，支持 Leader 选举与日志复制，保证多副本数据一致性。
  - 通过 Leader 节点提供线性一致性读写（强一致性）。
  - 持久化 Raft 状态机日志至 Bitcask 存储，支持节点崩溃恢复。

#### **5. 事务与并发控制**

[具体命令及案例](./doc/Transaction.md)

- **隔离级别**：
  - 支持可重复读（Repeatable Read），确保事务内多次读取结果一致。
  - 读写事务支持写冲突检测。
- **MVCC 多版本管理**：
  - 数据版本化存储，允许时间点快照查询（如历史数据回溯）。
  - 仅读事务无需锁竞争，直接访问快照版本。

#### **6. 网络通信**

- **高并发通信框架**：
  - 基于 Go 的 `goroutine` 与 `channel` 实现异步消息处理。
  - 自定义二进制协议（Header + Body 格式），支持高效序列化与反序列化事件。
  - 事件类型：客户端 SQL 请求、Raft 日志同步、集群心跳检测。

#### **7. 扩展性设计**

- **可插拔存储引擎**：
  - 默认集成 Bitcask，支持替换为内存存储（`BTreeMap`）及其他引擎适配不同场景。
- **轻量级部署**：
  - 单节点模式（无需分布式依赖）与集群模式一键切换。

### 启动方式

- 客户端

```go
cd client
go run ./main.go ./client.go
```

- 服务端单机

```go
go run ./main.go --config ./config/db.yaml
```

- 服务端集群（WINDOWS）

```go
./clusters/run.bat
```

- 服务端集群（LINUX）

```go
./clusters/run.sh
```

### 客户端命令

- !tables 查看所有表
- !table tablename 查看tablename表结构
- !status 查看raft节点状态


