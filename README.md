# spark-film-recommendation

## docker-compose 示例

建立简单的standalone模式集群:

    docker-compose up

## pyspark 示例

运行pyspark shell需进入一个容器中:

    docker exec -it master /bin/bash
    pyspark

## 推荐系统 示例

进入master node容器中：

    docker exec -it master /bin/bash

进入本地文件挂载的HDFS目录中：

    cd /tmp/data

spark-submit运行电影推荐代码：

    spark-submit train_model.py arg1 arg2 arg3 arg4

    #arg1取[0,1]表示是否需要训练新的模型，传入0时会从HDFS中加载已经保存的模型
    #arg2取[0,1]表示模型训练过程中是否需要网格搜索最优超参数
    #arg3是指定用户的ID字符串，不同ID间以”,”分隔
    #arg4是为每个用户推荐的电影数量

查看推荐结果：

    cat recommend_result.txt



