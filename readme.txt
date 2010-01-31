20100131
	hbase rest+guice  =  cloudfaseweb
	study hbase rest;
	study guice2.0
	study design mvc4hbaseandguice
	
20090312
	测试驱动，完成测试的过程中完善 myhbasetemplate
	

20090224
	
	完成一个demo(界面用ext)
	再完成一个产品。
	
	TODO lucene and hbase
	
20090219
	Transient ok
     增加ant build.xml
 	email 反转 直接作为key;
	
20090130
	使用默认名称，不同的包，同一个类名会出现混淆，不便于复用。
	表明使用类名（含包名，如果名称长度限制，使用hashcode）
	引入临时Annotation,所有临时字段必须申明；
		
		
20090127
			BeanInfo beanInfo = Introspector.getBeanInfo(type);
			PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

20090119
	支持boolean
	
	BeanUtils.populate(bean,   map)
	
20090115
	查询的时候可以使用列或者属性；要进行验证列属性
	

20090114
	实例化必须有构造子；
	user 用email的md5作为key,可以直接给ID赋值；不走配置注解文件；
	//覆盖默认的ID设置
	@ID(md5FieldName="name",value=IdType.MD5)
	private String id;
	完成findUserByName(String vlaue)
	
20090113
	save update get find must:
	hbaserow->object
	object->hbaserow
	hbaswrows->objects
	objects->hbaserows
	
	delete ->
	object->id
	objects->ids
	
	convertBean重构完成	
		
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
		filter-word autokey ok;
	  	模型的属性 crud 同步 hbase ok;
	2、常用查询，数据更改。TODO ok