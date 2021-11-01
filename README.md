# 191870294-朱云佳-作业6

**环境说明：windows使用intellij创建maven项目，环境配置与作业5相同，调试完毕后进入WSL+hdfs分布式环境运行**

[TOC]

## 解题思路

标点、停词、大小写、数字和单词长度的处理沿用了作业5wordcount，此处不再赘述

### Map

map输出的键值对是单词和所在文件名

![image-20211101222555560](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101222555560.png)

### Reduce

reduce函数里定义hashmap类型的变量，用于存放输入reduce的特定单词在各文件中的出现次数。hashmap的键为文件名，值为出现次数。

#### 统计次数

<img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101223018125.png" alt="image-20211101223018125" style="zoom: 67%;" />

若键不存在于map中，则创建并初始化值为1；否则更新已有的值

#### 按词频排序

![image-20211101223143256](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101223143256.png)

map转entrySet之后，定义Collectionn.sort降序排列，最后用StringBuilder统一结果写入

## 实验结果

完整结果参见result文件夹part-r-00000，这里显示本地intellij和WSL分布式运行的结果

### 本地intellij

![image-20211101193039920](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101193039920.png)

### WSL分布式

用`hadoop jar`命令执行

![image-20211101200603438](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101200603438.png)

显示运行成功

![image-20211101200615725](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101200615725.png)

发现hdfs系统上已生成结果文件

![image-20211101200808225](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101200808225.png)

`cat`查看生成结果:

![image-20211101201219804](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101201219804.png)(虽然由于行宽限制输出有点混乱但是)结果与本地一致！

![image-20211101201203741](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101201203741.png)

## 问题及解决方法

很快就完成了coding的框架，只是发现输出的时候：一是多余写入了"#1"，二是排序功能无法正常显示（后来推测与前者有关）

<img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101114028304.png" alt="image-20211101114028304" style="zoom:50%;" />

调整了context.write的输出（加入序列号）看起来，似乎是reduce一波之后，又来了一波：

<img src="C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101190450235.png" alt="image-20211101190450235" style="zoom:50%;" />

用println调试法--看起来它将之前的结果又统计了一遍（这里指的是shakespeare-alls-11.txt#2这种StringBuilder的内容，以下的print结果验证了猜想）![image-20211101191349641](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101191349641.png)

（其实花了9个小时断断续续调试才发现了上述端倪）...找来找去应该是combiner的锅（由于复用了wordcount的代码，没有去掉combiner，一开始也觉得不用去掉）

![image-20211101192054831](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101192054831.png)

于是复习了combiner的作用（看来在这个情景下使用combiner会导致reduce的时候再统计一遍，因为第一波combiner过后value类型是Text）

![image-20211101192145642](C:\Users\zyj\AppData\Roaming\Typora\typora-user-images\image-20211101192145642.png)

看来...要慎重考虑combiner的使用，解决方法就是注释掉combiner



