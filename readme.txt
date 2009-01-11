20090111
	Map 支持 String int long Date ;其他类型采用序列化
	如果数据无须检索，使用序列化存放； ok
20090110
	支持 Map and Date
20090108
	属性名称中间带":" hbase的特有类型Map 列存储 column:column=column: ok
	
	
20090107
	RichDomain
	/etc/security/limits.conf  
	*   -    nofile  32768
	crud hbase object:存储hbase对象；
	单个对象的crud与对象在一起，多个对象的crud放入manager;完成create ,没有测试 测试 ok
	
20090106
	23:01
	完成继承
	column 重复检测  ok
	
20090106 01:08

	1、功能和测试案例ok。
		模型的属性 crud 同步 hbase ok;
	2、常用查询，数据更改。TODO ok