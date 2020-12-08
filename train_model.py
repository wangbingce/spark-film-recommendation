from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS, Rating, MatrixFactorizationModel
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.linalg import DenseVector
import sys

def alsModelEvaluate(model, testing_rdd):
    predict_rdd = model.predictAll(testing_rdd.map(lambda r: (r[0], r[1])))
    #print(predict_rdd.take(5))
    predict_actual_rdd = predict_rdd.map(lambda r: ((r[0], r[1]), r[2])) \
        .join(testing_rdd.map(lambda r: ((r[0], r[1]), r[2])))

    #print(predict_actual_rdd.take(5))
    metrics = RegressionMetrics(predict_actual_rdd.map(lambda pr: pr[1]))

    #print("MSE = %s" % metrics.meanSquaredError)
    print("RMSE = %s" % metrics.rootMeanSquaredError)

    return metrics.rootMeanSquaredError


def train_model_evaluate(training_rdd, testing_rdd, rank, iterations, lambda_):
    model = ALS.train(training_rdd, rank, iterations, lambda_)

    rmse_value = alsModelEvaluate(model, testing_rdd)

    return (model, rmse_value, rank, iterations, lambda_)


def train_model(need_grid_search, ratings_datas):
    training_ratings, testing_ratings = ratings_datas.randomSplit([0.8, 0.2])

    rank, iterations, lambda_ = 10 , 10 , 0.001
    if need_grid_search == 1:
        metrix_list = [train_model_evaluate(training_ratings, testing_ratings, param_rank, param_iterations , param_lambda)
                    for param_rank in [10, 20]
                    for param_iterations in [10, 20]
                    for param_lambda in [0.001, 0.01]
                    ]
        sorted(metrix_list, key=lambda k: k[1], reverse=False)
        model, rmse_value, rank, iterations, lambda_ = metrix_list[0]
        print("The best parameters, rank=%s, iterations=%s, lambda_=%s" %(rank ,iterations , lambda_))
    model = ALS.train(ratings_datas, rank, iterations, lambda_)
    # 保存模型
    model.save(sc, "./data/als_model")
    return model

if __name__ == "__main__":
    need_train_model = int(sys.argv[1])
    need_grid_search = int(sys.argv[2])
    user_id_list = sys.argv[3]
    recommend_num = int(sys.argv[4])
    user_id_list = user_id_list.split(",")
    spark = SparkSession.builder \
        .appName("Movie_Recommend") \
        .master("spark://master:7077") \
        .getOrCreate()
    sc = spark.sparkContext
    if (need_train_model == 1):
        raw_ratings_rdd = sc.textFile("./data/rating_data.csv" )
        ratings_rdd = raw_ratings_rdd.map(lambda line: line.split(';')[0:3])
        ratings_datas = ratings_rdd.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))
        # 查看评分数据中有多少电影
        # print(ratings_datas.map(lambda x: x[1]).distinct().count())

        # 查看评分数据中有多少用户
        # print(ratings_datas.map(lambda x: x[0]).distinct().count())
        model = train_model(need_grid_search, ratings_datas)
    else :
        model =MatrixFactorizationModel.load(sc, "./data/als_model")
    movie_detail_rdd = sc.textFile("./data/key_word_data.csv" )
    movie_detail = movie_detail_rdd.map(lambda line:line.split(";")).map(lambda a:(int(a[0]),[a[1],a[2]])).collectAsMap()
    filename = 'recommend_result.txt'
    with open(filename, 'w') as file_object:
        for id_ in user_id_list:
            recommend_list = model.recommendProducts(int(id_), recommend_num)
            file_object.write("对用户： "+str(id_)+"\n")
            for element in recommend_list:
                file_object.write("    推荐电影："+str(movie_detail[element[1]][0])+"，电影关键词："+str(movie_detail[element[1]][1])+"\n")
            file_object.write("\n")