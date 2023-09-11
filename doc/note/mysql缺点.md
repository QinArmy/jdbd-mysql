1. start transaction statement 没有事务隔离级别
2. 不能查询当前事务的隔离级别和 WITH CONSISTENT SNAPSHOT 状态
3. 没有为 将 Boolean 作为一种类型,这是非常笨的设计.
4. Query Attributes 打开了一道非常美丽的门,支持绑定特定的类型,但只能使用 mysql_query_attribute_string(), 门是很好看，里面是狗屎,愚蠢.
5. 若接收 Big payload 时服务器响应错误那将无法接收错误包.
6. prepare 协议 批量操作不能像 postgre 那样,所以 网络效率底.
7. 包类型的确认太依赖于上下文,所以在做错误 skip 包麻烦
8. 关键 变量 字符集 时区等,用户可能通过 sql 更改 这给 driver 解析 结果集带来麻烦，这可能造成解析错误.
9. mysql 结果集类型的非常不好确定，简直是愚蠢,比如 INFORMATION_SCHEMA.COLUMNS 的 DATA_TYPE 列居然返回 blob 类型,相比较而方,则
   postgre 这确定类型这个方面很确定.



