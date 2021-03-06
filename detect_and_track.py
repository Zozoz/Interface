#!/usr/bin/env python
# encoding: utf-8


from collections import Counter
import datetime
import os
import math
import MySQLdb
import redis
import pymongo
from pymongo import MongoClient


class BurstDetect(object):

    def __init__(self):
        self.threshold = 3
        self.conn = MySQLdb.connect(host='202.119.84.47', user='root', passwd='qwert123456', db='weibo_test', port=3306)
        self.cur = self.conn.cursor()
        self.R = redis.Redis(host='localhost', port=6379, db=1)
        self.R.flushdb()
        mongodb = MongoClient('localhost', 27017)
        self.db = mongodb['weibo']
        self.col = self.db['burst_events']
        self.col.create_index([('timestamp', pymongo.ASCENDING)])
        self.col.delete_many({})
        self.col_t2id = self.db['tweets2id']
        self.col_t2id.delete_many({})
        self.col_track = self.db['tracking_events']
        self.col_track.create_index([('timestamp', pymongo.ASCENDING)])
        self.col_track.delete_many({})
        self.burst_word = None
        self.word2id = None
        self.id2word = None
        self.records = None
        self.vec = None
        self.clu = None
        self.timestamp = None

    def create_table(self):
        sql = """
            create table if not exists tweets(
                id int not null auto_increment,
                primary key(id),
                username varchar(50) not null,
                timestamp datetime not null,
                pageurl varchar(100) not null,
                location varchar(50) not null,
                is_v int default 0,
                type int default 0,
                content varchar(200) not null,
                repost int default 0,
                remark int default 0,
                support int default 0,
                source char(20) not null,
                sentiment int default 0,
                pos float default 0.0,
                neu float default 0.0,
                neg float default 0.0,
                score float default 0.0,
                sentiment_show varchar(200) default '',
                index(timestamp)
            )engine=InnoDB charset='utf8';
        """
        self.cur.execute(sql)
        self.conn.commit()

    def format_time(self, atime):
        atime = atime.strip('\r\n').strip('\n').split()
        atime = atime[0].split('-') + atime[1].split(':')
        return [int(tt) for tt in atime]

    def split_content(self, quzao_file, fenci_file):
        """
        split content by hour slice
        """

        base_path = '/home/poa/data/yingji/'
        quzao_files = sorted(os.listdir(quzao_file))
        content_files = sorted(os.listdir(fenci_file))

        cnt = 0

        for quzao_file, content_file in zip(quzao_files, content_files):

            contents = open(base_path + 'fenci/' + content_file + '/' + content_file + '.txt', 'r')
            timestamps = open(base_path + 'quzao/' + quzao_file + '/timelist.txt', 'r')
            pageurls = open(base_path + 'quzao/' + quzao_file + '/pageurl.txt', 'r')
            usernames = open(base_path + 'quzao/' + quzao_file + '/userid.txt', 'r')
            sources = open(base_path + 'quzao/' + quzao_file + '/source.txt', 'r')
            locations = open(base_path + 'quzao/' + quzao_file + '/location.txt', 'r')

            f = open('pp.txt', 'w')
            for content, timestamp, pageurl, username, source, location in \
                    zip(contents, timestamps, pageurls, usernames, sources, locations):
                content = content.strip('\r\n').strip('\n').decode('utf-8').encode('utf-8')
                timestamp = timestamp.strip('\r\n').strip('\n').encode('utf-8')
                pageurl = pageurl.strip('\r\n').strip('\n').encode('utf-8')
                username = username.strip('\r\n').strip('\n').decode('utf-8').encode('utf-8')
                source = source.strip('\r\n').strip('\n').decode('utf-8').encode('utf-8')
                location = location.strip('\r\n').strip('\n').decode('utf-8').encode('utf-8')
                f.write(content + '\n')
                f.write(timestamp + '\n')
                f.write(pageurl + '\n')
                f.write(username + '\n')
                f.write(source + '\n')
                f.write(location + '\n')

                sql = 'insert into tweets(content, timestamp, pageurl, username, source, location) values (%s, %s, %s, %s, %s, %s)'
                self.cur.execute(sql, (content, timestamp, pageurl, username, source, location))

                cnt += 1
                print cnt

            self.conn.commit()

    def get_slice_content_from_mysql(self, s_time, e_time):
        sql = "select id, timestamp, pageurl, content from tweets where timestamp>=%s and timestamp<%s"
        self.cur.execute(sql, (s_time, e_time))
        results = self.cur.fetchall()
        return results

    def process(self, nowdatetime):
        # get tweets from mysql
        # nowdatetime = datetime.datetime.now()
        if nowdatetime.minute % 1 == 0:
            e_time = nowdatetime.strftime('%Y-%m-%d %H:%M')
            s_time = (nowdatetime - datetime.timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M')
            results = self.get_slice_content_from_mysql(s_time, e_time)

        self.timestamp = e_time
        print s_time, e_time

        # count term frequency of words
        tf = dict()
        self.records = []
        for row in results:
            content = row[3].split()
            self.records.append([row[0], row[1], row[2], content])  # id, timestamp, pageurl, content
            for item in content:
                tf[item] = tf[item] + 1 if item in tf else 1

        # save tf to redis
        try:
            pre_gap_tf = eval(self.R.lpop(s_time))
        except:
            print 'here'
            pre_gap_tf= dict()
            pass
        self.R.lpush(e_time, tf)

        # compute increasing rate of words
        inr = dict()
        for k, v in tf.items():
            inr[k] = v * 1.0 / pre_gap_tf[k] if k in pre_gap_tf else v

        # get burst word by unoin
        tf_trun = dict(sorted(tf.items(), key=lambda d: d[1], reverse=True)[:15])
        inr_trun = dict(sorted(inr.items(), key=lambda d: d[1], reverse=True)[:15])
        self.burst_word = list(set(tf_trun.keys() + inr_trun.keys()))

        # word id mapping
        self.word2id = dict()
        self.id2word = dict()
        for i in xrange(len(self.burst_word)):
            self.word2id[self.burst_word[i]] = i
            self.id2word[i] = self.burst_word[i]

        self.word2vec()
        self.cluster(self.vec)
        self.get_burst_event(self.clu)

    def word2vec(self):
        length = len(self.burst_word)
        self.vec = []
        for record in self.records:
            content = record[3]
            cont_vec = [0] * length
            for item in content:
                if item in self.word2id:
                    cont_vec[self.word2id[item]] = 1
            self.vec.append(cont_vec)

    def get_multify(self, vec1, vec2):
        sum = 0
        for i in xrange(len(vec1)):
            sum += vec1[i] * vec2[i]
        return sum

    def is_similar(self, vec1, vec2):
        len1 = sum(vec1)
        len2 = sum(vec2)
        td = min(len1, len2) - self.get_multify(vec1, vec2)
        if len1 > 3 and len2 > 3:
            if td <= 2.0:
                return True
            else:
                return False
        else:
            if td < 2.0:
                return True
            else:
                return False

    def cluster(self, input_vec):
        centroid = []
        cnt = 0
        while True:
            if sum(input_vec[cnt]) >= 4:
                centroid.append([cnt])
                cnt += 1
                break
            cnt += 1
            if cnt == len(input_vec):
                break
        for i in xrange(cnt, len(input_vec)):
            if sum(input_vec[i]) < 4:
                continue
            flag1 = True
            for j in xrange(len(centroid)):
                flag2 = True
                for item in centroid[j]:
                    if not self.is_similar(input_vec[i], input_vec[item]):
                        flag2 = False
                        break
                if flag2:
                    centroid[j].append(i)
                    flag1 = False
                    break
            if flag1:
                centroid.append([i])
        self.clu = centroid
        print '-' * 100
        print ' '.join(self.burst_word)
        print 'the number of doc is ', len(input_vec)
        print 'centroid number is ', len(centroid)

    def get_burst_event(self, centroid):
        centroid.sort(key=lambda d: len(d), reverse=True)
        for i in xrange(len(centroid)):
            leng = len(centroid[i])
            if leng < 10:
                break
            print 'the length of centroid %d is %d' % (i, len(centroid[i]))

            # find keywords whose frequency is more than leng*0.3
            tweets = []
            words = dict()
            for item in centroid[i]:
                print self.records[item][0], ' '.join(self.records[item][3])
                tweets.append(self.records[item][0])
                for word in set(self.records[item][3]):
                    if word in self.burst_word:
                        words[word] = words[word] + 1 if word in words else 1
            words = {k: v for k, v in words.items() if v >= leng * 0.5}
            for k, v in words.items():
                print k, ': ', v,
            print

            # write burst events to mongodb
            burst_event = dict()
            burst_event['timestamp'] = self.timestamp
            burst_event['burst_words'] = words
            burst_event['tweets_id'] = tweets
            burst_event['burst_tweets_count'] = len(centroid[i])
            burst_event['sum_tweets_count'] = len(self.vec)

            # calculate sentiment
            sql = 'select sum(pos) from tweets where id in %s and pos > neu and pos > neg'
            self.cur.execute(sql, (tweets, ))
            ans = self.cur.fetchone()
            sentiment = ans[0]
            burst_event['sentiment_pos'] = sentiment

            sql = 'select sum(neu) from tweets where id in %s and neu > pos and neu > neg'
            self.cur.execute(sql, (tweets, ))
            ans = self.cur.fetchone()
            sentiment = ans[0]
            burst_event['sentiment_neu'] = sentiment

            sql = 'select sum(neg) from tweets where id in %s and neg > neu and neg > pos'
            self.cur.execute(sql, (tweets, ))
            ans = self.cur.fetchone()
            sentiment = ans[0]
            burst_event['sentiment_neg'] = sentiment

            result = self.col.insert_one(burst_event)

            # burst event tracking
            self.get_tracking_event(result.inserted_id)

    def get_tracking_event(self, burst_id):
        cur_event = self.col.find_one({'_id': burst_id})
        cur_words = cur_event['burst_words']
        cur_words = {k.encode('utf-8'): v for k, v in cur_words.items()}
        results = self.col.find({'timestamp': {'$gt': '2015-06-17'}})
        pre_events = []
        for res in results:
            if res['_id'] != burst_id:
                pre_events.append(res)
        flag = False
        if pre_events:
            # calculate cos distance between current-event and previous-events
            cos_values = []
            cur_sum = math.sqrt(sum([v**2 for v in cur_words.values()]))
            for i in xrange(len(pre_events)):
                pre_words = {k.encode('utf-8'): v for k, v in pre_events[i]['burst_words'].items()}
                pre_sum = math.sqrt(sum([v**2 for v in pre_words.values()]))
                com_words = set(pre_words.keys()) & set(cur_words.keys())
                com_sum = sum([pre_words[word] * cur_words[word] for word in com_words])
                cos_value = com_sum / (cur_sum * pre_sum)
                if cos_value > 0.3:
                    cos_values.append((cos_value, i))
            if cos_values:
                cos_values.sort(key=lambda d: d[0], reverse=True)

                vote = dict()
                for event in cos_values:
                    k = pre_events[event[1]]['parent_id']
                    vote[k] = vote[k] + 1 if k in vote else 1
                vote = sorted(vote.items(), key=lambda d: d[1], reverse=True)
                # if all vote of parent_id is 1, use the one whose cos_value is biggest
                # else use the one whose vote is most.
                if vote[0][1] == 1:
                    parent_id = pre_events[cos_values[0][1]]['parent_id']
                else:
                    parent_id = vote[0][0]
                parent_id = pre_events[cos_values[0][1]]['parent_id']
                parent = self.col_track.find_one({'_id': parent_id})
                pre_words = parent['burst_words']
                burst_words = dict(Counter(pre_words) + Counter(cur_words))
                self.col_track.replace_one({'_id': parent['_id']}, {
                    'timestamp': self.timestamp,
                    'burst_words': burst_words,
                    # 'sentiment': parent['sentiment'] + cur_event['sentiment'],
                    'burst_tweets_count': parent['burst_tweets_count'] + cur_event['burst_tweets_count'],
                    'sum_tweets_count': parent['sum_tweets_count'] + cur_event['sum_tweets_count'],
                    'burst_events_objectid': parent['burst_events_objectid'] + [burst_id]
                })
                flag = True
        if not flag:
            parent = self.col_track.insert_one({
                'timestamp': self.timestamp,
                'burst_words': cur_words,
                # 'sentiment': cur_event['sentiment'],
                'burst_tweets_count': cur_event['burst_tweets_count'],
                'sum_tweets_count': cur_event['sum_tweets_count'],
                'burst_events_objectid': [burst_id]
            })
            parent_id = parent.inserted_id
        print 'parent_id: ', parent_id
        self.col.update_one({'_id': burst_id}, {
            '$set': {'parent_id': parent_id}
        })

    def test(self):
        day = range(20, 22)
        hour = range(0, 24)
        minute = range(0, 60, 10)
        for d in day:
            for h in hour:
                for m in minute:
                    times = datetime.datetime(2015, 6, d, h, m)
                    self.process(times)


if __name__ == '__main__':
    burst = BurstDetect()
    burst.test()
