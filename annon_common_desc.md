# Introduction #

@Table name ：只能用于类；<br>
@Cache 否启用cache：可以用于类，属性；<br>
@Inmemory  常驻内存：可以用于类，属性；<br>
@Index 用于bloomfilter,查询；<br>
@Column name:字段描述，:符号只能用于最后一个位置，没有的话会自动添加；<br>
@One2Many name @Many2One name 必须配对使用；有多个many,属于不同的one,使用bloomfilter批量查x<br>
@Id ParentChild 规则，利用ID规则进行查询，不能分页，不适用child数据较多的情况；组成parent-child<br>



<h1>Details</h1>

Add your content here.  Format your content with:<br>
<ul><li>Text in <b>bold</b> or <i>italic</i>
</li><li>Headings, paragraphs, and lists<br>
</li><li>Automatic links to other wiki pages