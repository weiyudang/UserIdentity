from pyspark.sql   import SparkSession
import os,time,tqdm
from pyspark.sql  import Row
def toRow(row, label, cname):
        rowDict = row[1].asDict()
        user_id = row[0]

        cols = [
                'created_at',
                'attitudes_count',
                'reposts_count',
                'comments_count',
                'favorited',
                u'mid'
        ]

        data = {col: rowDict.get(col) for col in cols}
        import time
        if 'retweeted_status' in rowDict:
                retweeted = rowDict['retweeted_status']
                if retweeted != None:
                        retweeted = retweeted.asDict()
                        # comments_count reposts_count  attitudes_count
                        ret_comm = retweeted.get("comments_count")
                        re_reposts_count = retweeted.get("reposts_count")
                        re_attitudes_count = retweeted.get('attitudes_count')
                        mid = retweeted.get('mid')
                else:
                        re_reposts_count = -1
                        ret_comm = -1
                        re_attitudes_count = -1
                        mid = '0'
                if retweeted != None and 'text' in retweeted:
                        retweeted = retweeted['text']
                else:
                        retweeted = ' '

        if 'text' in rowDict:
                text = rowDict['text']

        if 'created_at' in rowDict:
                dt = ToDateTime(rowDict.get('created_at'))
        else:
                dt = '2018-01-01 00:00:00'
        data.update({'uid': user_id,
                     'label': label, 'identity': cname,
                     'retweeted': retweeted, 'text': text, 'created_at': dt,
                     're_attitudes_count': re_attitudes_count,
                     're_comm_cnt': ret_comm,
                     're_reposts_count': re_reposts_count,
                     're_mid': mid,
                     'dt': dt[:7]})

        return Row(**data)


def ToDateTime(created_at):
        if created_at != None:
                created_at = created_at[:19] + created_at[25:]

                tm = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(created_at))
        else:
                tm = '2018-01-01 00:00:00'
        return tm




def getClassNames():
        paths = '/data/home/cmp/jupyter/data/user_identify/'
        filenames = []
        from os.path import join, getsize
        classNames = []
        for root, dirs, files in os.walk(paths):

                class_name = root.split('/')[7]

                if len(class_name) > 1:
                        classNames.append(class_name)

                filenames.extend([join(root, name) for name in files])
        return list(set(classNames))




if __name__=='__main__':
        ## data format convert and write into hive
        classNames = getClassNames()
        sqlContext=SparkSession.builder.getOrCreate()
        for code, cname in tqdm.tqdm_notebook(enumerate(classNames)):
                label = code + 1
                pth = '/team/cmp/hive_db/cmp_tmp/user_identify/{class_name}/**'.format(class_name=cname)
                cname = unicode(cname, 'utf-8')
                print label, cname
                df = sqlContext.read.json(pth)
                df = df.select('uid', 'microblogs').filter(
                        'size(microblogs)>0').rdd.flatMapValues(
                        lambda x: x).map(lambda row: toRow(row, label, cname)).toDF(sampleRatio=0.1)
                df.write.saveAsTable('cmp_tmp_user_identification', mode='append', partitionBy='dt')