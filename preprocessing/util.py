import jieba
import pyspark.sql.functions  as F
from spark_udf import *
from pyspark.ml.feature import   CountVectorizer,StopWordsRemover
from pyspark.sql  import SparkSession
import time
from pyspark.sql import Row
import pyspark.sql.functions  as F
import os
import tqdm

class descUserDistribution(object):
        '''
        retweet_des=descUserDistribution(cond='length(retweeted)>1')(df)
        tweet_des=descUserDistribution(cond="retweeted==' '")(df)
        '''

        def __init__(self, cond='length(retweeted)>0'):
                self.cond = cond

        def fit(self, df):
                if self.cond:
                        df_self = df.filter(self.cond)
                else:
                        df_self = df

                self.data_num = df_self.count()
                print 'Data numbers:', self.data_num
                self.label_count = df_self.groupBy('identity').count().toPandas().set_index('identity')
                self.user_count = df_self.groupBy('identity').agg(
                        F.countDistinct('uid').alias('cnt')).toPandas().set_index('identity')

        def __call__(self, df):
                self.fit(df)
                return self





def getStopWords():
        '''
        :return: stopwords list
        '''
        stopwords_file = '../data/哈工大停用词表.txt'
        with open(stopwords_file) as f:
                stopwords = f.read()
                stopwords = unicode(stopwords, 'utf-8')
                stopwords = stopwords.split('\n')
        return stopwords




from pyspark import SparkContext


class UserIdentity(object):
        '''
        user_iden=UserIdentity(retweet=False)
        result_df=user_iden()
        result_df.repartition(1).write.csv('/team/cmp/hive_db/cmp_tmp/UserIdentity.csv')
        '''

        def __init__(self, test=True, tweet=True, retweet=True, minDF=3, minTF=5,stopwords=[]):
                self.tweet = tweet
                self.retweet = retweet
                self.minDF = minDF
                self.minTF = minTF
                self.stopwords=stopwords
                self.test=test

        def fit(self):
                sqlContext=SparkSession.builder.getOrCreate()
                if self.test:
                        df = sqlContext.sql("select * from cmp_tmp_user_identification where dt='2014-01'")
                else:
                        df = sqlContext.sql("select * from cmp_tmp_user_identification")

                if self.tweet and self.retweet:
                        df = df.withColumn('content', F.concat('text', 'retweeted'))
                elif self.tweet:
                        df = df.filter("retweeted==' '")
                        df = df.withColumn('content', F.col('text'))
                elif self.retweet:
                        df = df.filter('length(retweeted)>1')
                        df = df.withColumn('content', F.col('retweeted'))

                df = df.withColumn('content', textCut(clean_text('content')))
                ##stopwords
                remover = StopWordsRemover(inputCol="content", outputCol="words", stopWords=self.stopwords)
                df = remover.transform(df)
                ## 清理空字符
                df = df.filter('size(words)>0')
                self.sentence_length_distribution = df.selectExpr('size(words) as wz').groupBy(
                        'wz').count().toPandas().set_index('wz').sort_index()
                ###vec
                cv = CountVectorizer(inputCol='words', outputCol='vertors', minDF=self.minDF, minTF=self.minTF)
                model_cv = cv.fit(df)
                word2bag = model_cv.vocabulary
                self.baglen = len(word2bag)
                self.dictionary = dict(zip(word2bag, ['W' + str(i) for i in range(1, self.baglen)]))
                sc = SparkContext.getOrCreate()
                diction = sc.broadcast(self.dictionary)

                ## to English format to GCN
                df = df.withColumn('words_space', toSpaceSplit('words'))
                result_df = df.selectExpr('uid,label,identity,words_space'.split(','))
                ##aggregate to user level
                result_df = result_df.groupBy(
                        'uid', 'label', 'identity').agg(
                        F.collect_list(
                                'words_space').alias('uid_words'))
                result_df = result_df.withColumn('uid_words', concat_uid('uid_words'))
                return result_df

        def __call__(self):
                return self.fit()



