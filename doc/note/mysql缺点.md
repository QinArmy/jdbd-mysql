1. start transaction statement 没有事务隔离级别
2. 不能查询当前事务的隔离级别和 WITH CONSISTENT SNAPSHOT 状态
3. 没有为 将 Boolean 作为一种类型,这是非常笨的设计.
4. Query Attributes 打开了一道非常美丽的门,支持绑定特定的类型,但只能使用 mysql_query_attribute_string(), 门是很好看，里面是狗屎,愚蠢.



