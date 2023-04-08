# bigdata

## 项目背景

云计算课程：大数据的推荐以及热门gitee项目的数据统计

## 项目实现

### 环境搭建

Spark 3.x 、Maven 3.x 、 Jdk1.8、zookeeper3.6

### 整体架构

![image](https://user-images.githubusercontent.com/58102870/230713243-9a9d014b-c663-47a4-9d19-8e936937ca71.png)

二度好友的推荐：
![image](https://user-images.githubusercontent.com/58102870/230713255-69e47511-610a-4e20-bc0e-f5c21d86084c.png)

> 二度推荐
> 目的：把好友的好友推荐给你
> 方法：通过GraphX建图，用户为点，存在关注、被关注关系就建立边，每个顶点的属性Attr为Map(dstId->distance)，初始化为Map( 该顶点ID->0) 。然后进行两次迭代求解二度关系．
> 第一次迭代: 遍历每条边，将dst顶点属性dstAttr中的跳数字段标记为1 发给src顶点，src收到后合并到顶点属性srcAttr里．
> ![image](https://user-images.githubusercontent.com/58102870/230713304-575391bb-9c18-4064-acf6-7713ac5d497e.png)
> 第二次迭代: 遍历边筛选出dstAttr里面跳数为1 的Key-Value 发给对应的src顶点，并将dstId加入桥梁顶点，最后聚合这些消息得到所有2 跳邻居。
> ![image](https://user-images.githubusercontent.com/58102870/230713312-429e105c-25df-4c95-bd61-d34bc1305a93.png)

![image](https://user-images.githubusercontent.com/58102870/230713081-758da831-601d-401a-8cb6-d2f9b7985d1d.png)

### 实现细节

1. 利用多线程爬取最新的gitee开源仓库信息，来模拟实时数据源，为了追求爬虫的性能，分别比较了python和java的爬虫代码的效果。
其中java版本考虑了线程池的大小，通过while循环分批爬取，防止任务数过多线程阻塞，拒绝服务。
将爬取的数据封装后备份在本地，同时上传到HDFS，为了模拟数据涌入，采用的数据一条一条上传。

![image](https://user-images.githubusercontent.com/58102870/230713179-87459597-7f39-4a95-a2b7-fc57dbd48d7d.png)

2. 定时将HDFS中的最新数据取出，把Watch、Fork、Star进行排序后，更新ranks文件，并传回HDFS，后期前端进行展
示。

![image](https://user-images.githubusercontent.com/58102870/230713167-73bebf0f-595d-4da3-bf83-d212441a7c41.png)

![image](https://user-images.githubusercontent.com/58102870/230713170-2a408af9-f337-4bd8-914c-793dd8a74b57.png)

3. 每1分钟触发数据的计算，统计24小时中所有分类出现次数，一小时作为一个窗口进行统计。

![image](https://user-images.githubusercontent.com/58102870/230713146-2094ed57-6bb3-4850-90e3-6b2e532620fb.png)

4. 二度好友推荐
![image](https://user-images.githubusercontent.com/58102870/230713344-d12712b9-867b-418a-bdf1-5a1017636904.png)

![image](https://user-images.githubusercontent.com/58102870/230713349-bf87e0fc-9459-443f-97e6-4eeecb10ccea.png)

考虑到用户的关注不会频繁更新，那么用户推荐的结果就不应该频繁更新占用资源，考虑到频繁的查询，所以存入Redis中，每12小时更新一次。
![image](https://user-images.githubusercontent.com/58102870/230713359-3c370b29-bad8-46cc-8a69-f70e92a207c1.png)

### 运行结果

1. Rank榜单
![image](https://user-images.githubusercontent.com/58102870/230713408-dee6cb61-b8da-4494-9d25-aa7bd3798509.png)

2. 分类统计
![image](https://user-images.githubusercontent.com/58102870/230713417-9c1eb981-7345-40fb-adce-ae845f55490d.png)

3. Redis中推荐结果
![image](https://user-images.githubusercontent.com/58102870/230713388-37d21907-3207-48b2-ba98-c42ed9b06a8f.png)

