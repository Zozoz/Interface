#!/usr/bin/env python
# encoding: utf-8


import json
import bson
import MySQLdb
import pymongo
from pymongo import MongoClient


class Interface(object):

    def __init__(self):

        self.conn = MySQLdb.connect(host='202.119.84.47', user='root', passwd='qwert123456', db='weibo', port=3306)
        self.cur = self.conn.cursor()
        client = MongoClient('localhost', 27017)
        db = client['weibo']
        self.col = db['burst_events']
        self.col_track = db['tracking_events']


    # generate json

    def get_now_events(self, timestamp):
        # burst_events = col.find({'timestamp': {'$lt': '2015-06-20 20:50', '$gt': '2015-06-20 19:00'}})\
        #         .sort([('timestamp', pymongo.ASCENDING), ('burst_tweets_count', pymongo.DESCENDING)])
        burst_events = self.col.find({'timestamp': {'$gt': timestamp}})\
                .sort([('timestamp', pymongo.ASCENDING), ('burst_tweets_count', pymongo.DESCENDING)])
        events = dict()
        cnt = 1
        for event in burst_events:
            event['_id'] = str(event['_id'])
            event['parent_id'] = str(event['parent_id'])
            events[str(cnt)] = event
            cnt += 1

        events = json.dumps(events)
        return events

    def get_pre_events(self, s_time, e_time):
        s_time = '2015-06-20 19:00'
        e_time = '2015-06-20 20:50'
        burst_events = self.col.find({'timestamp': {'$lt': e_time, '$gt': s_time}})\
                .sort([('timestamp', pymongo.ASCENDING), ('burst_tweets_count', pymongo.DESCENDING)])
        events = dict()
        cnt = 1
        for event in burst_events:
            event['_id'] = str(event['_id'])
            event['parent_id'] = str(event['parent_id'])
            events[str(cnt)] = event
            cnt += 1

        events = json.dumps(events)
        return events

    def get_tracking_events(self, _id):
        event = self.col_track.find_one({'_id': bson.objectid.ObjectId(_id)})
        object_ids = event['burst_events_objectid']
        for i in xrange(len(object_ids)):
            object_ids[i] = str(object_ids[i])
        event['burst_events_objectid'] = object_ids
        event['_id'] = str(event['_id'])
        event = json.dumps(event)
        return event

    def get_single_event(self, _id):
        event = self.col.find_one({'_id': bson.objectid.ObjectId(_id)})
        event['_id'] = str(event['_id'])
        event['parent_id'] = str(event['parent_id'])
        event = json.dumps(event)
        return event

    def get_single_event_detail(self, _id):
        event = dict()
        sql = "select id, username, timestamp, pageurl, location, is_v, \
                score, content, pos, neu, neg from tweets where id=%s and content!=%s"
        self.cur.execute(sql, (_id, ''))
        try:
            _id, username, timestamp, pageurl, location, is_v, score, content, pos, neu, neg = self.cur.fetchone()
        except:
            return None
        else:
            event['id'] = _id
            event['username'] = username
            event['timestamp'] = str(timestamp)
            event['pageurl'] = pageurl
            event['location'] = location
            event['is_v'] = is_v
            event['score'] = score
            event['content'] = content
            event['pos'] = pos
            event['neu'] = neu
            event['neg'] = neg
            return json.dumps(event)




if __name__ == '__main__':
    test = Interface()
    event = test.get_tracking_events('569deb6935c0eb20602781fa')
    event = json.loads(event)
    ids = event['burst_events_objectid']
    event = test.get_events(ids)
    event = json.loads(event)
    tweets_id = event['89']['tweets_id']
    events = test.get_details(tweets_id)
    print events




    #burst_events = col.find()
    #for burst_event in burst_events:
    #    print burst_event['timestamp']
    #    for words in burst_event['burst_words']:
    #        print words.encode('utf-8'),
    #    print
    #    print '-' * 50
    #    for id_ in burst_event['tweets_id']:
    #        sql = "select content, timestamp from tweets where id=%s"
    #        cur.execute(sql, (id_, ))
    #        content, timestamp = cur.fetchone()
    #        print timestamp,
    #        print content.decode('utf-8').encode('utf-8')
    #    print


    #tracking_events = col_track.find().sort('timestamp')
    #cnt  = 1
    #for event in tracking_events:
    #    print cnt, 'aaaaa'
    #    cnt += 1
    #    print '-' * 30, event['timestamp'], '-' * 30
    #    tweets_id = []
    #    _ids = event['burst_events_objectid']
    #    for _id in _ids:
    #        a = col.find_one({'_id': _id})
    #        tweets_id += a['tweets_id']
    #    print 'sum : ', len(tweets_id)
    #    for k, v in event['burst_words'].items():
    #        print k.encode('utf-8'), v, ' | '
    #    print
    #    for _id in tweets_id:
    #        sql = "select content, timestamp from tweets where id=%s"
    #        cur.execute(sql, (_id, ))
    #        content, timestamp = cur.fetchone()
    #        print timestamp,
    #        print content.decode('utf-8').encode('utf-8')
    #    print
    #


