# BigData
常见大数据技术基本使用，包括Hadoop、ZooKeeper、Kafka、Spark、Hive、HBase、Flink等大数据技术。

### Wiki中包含常用技术整理！！！！

# 文件说明：
### 一、ConfigurationFiles 
  ConfigurationFiles文件中包含以下大数据技术框架的配置文件和其它说明
  #### Hadoop(2.8.5)
  #### ZooKeeper(3.4.10)
  #### Kafka(2.11-0.10.2.2)
  #### Spark
  #### Hive
  #### HBase
  #### Flink

### 二、Hadoop(版本：2.8.5)
  Hadoop示例工程中是基于SpringBoot框架实现的，包括HDFS常用基本操作方法和MapReduce对wordcount单词统计
  HDFS操作：
  * 1、根据配置参数获取HDFS文件系统；
  * 2、将相对路径转化为HDFS文件路径；
  * 3、判断文件或者目录是否在HDFS上已存在；
  * 4、创建HDFS目录；
  * 5、本地文件上传至HDFS；
  * 6、获取HDFS上面的某个路径下面的所有文件或目录（不包含子目录）信息；
  * 7、从HDFS下载文件至本地；
  * 8、打开HDFS上面的文件并返回InputStream；
  * 9、打开HDFS上面的文件并转换为Java对象；
  * 10、重命名HDFS上的文件名；
  * 11、删除HDFS文件或目录；
  * 12、获取某个文件在HDFS集群的位置；

### 三、Kafka(版本：2.11-0.10.2.2)
  Kafka示例工程中是基于SpringBoot框架实现的，包括Kafka基本配置信息，以及发送消息实现和接收消息实现，主要内容如下：
  * 1、application-dev.properties配置文件中包含Kafka消息producer发送端和consumer接收端的基本配置和配置说明；
  * 2、消息生产者SimpleProducer实现和消息消费者SimpleConsumer实现，在该实例中，Kafka发送/接收的消息均是简单字符串，其本质是使用Kafka自带的方法：StringDeserializer和StringDeserializer来编码/解码消息；
  * 3、发送/接收自定义类型消息(例如可以是Java对象)，直接发送和接收Java对象，自定义消息编码器为ObjectSerializer，自定义消息解码器为ObjectDeserializer；\
  ***注意：改变消息编码器和解码器之后需要清空Topic中原有消息或者使用新的Topic，否则原来的字符串消息在反序列化时会出现异常，切记~~~***
  * 4、 多消费者组消费同一条消息；
  * 5、批量消费消息，在@KafkaListener注解中设置containerFactory参数可以批量消费消息。
