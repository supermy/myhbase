# Introduction #

Cloudera and Hbase install

# Details #

首先需要先下载HBase最新且稳定的版本：http://www.apache.org/dyn/closer.cgi/hadoop/hbase
然后把它解压，把解压下来的文件移动到一个目录下如/home/user/hbase
$ cd /home/user/hbase
进入此目录

$ vi conf/hbase-env.sh
> export JAVA\_HOME=/usr/lib/jvm/java-6-sun-1.6.0.03
编辑 conf/hbase-env.sh 文件,修改JDK的路径

$ vi conf/regionservers
输入你的所有HBase服务器名

$ bin/start-hbase.sh
启动hbase
启动后可以在 logs/目录下看到不少logs文件

也可以输入以下指令进入HQL指令模式
$ bin/hbase shell
$ bin/stop-hbase.sh
关闭HBase服务

Add your content here.  Format your content with:
  * Text in **bold** or _italic_
  * Headings, paragraphs, and lists
  * Automatic links to other wiki pages