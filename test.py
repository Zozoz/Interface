#!/usr/bin/env python
# encoding: utf-8


import json
import bson
import MySQLdb
import pymongo
from pymongo import MongoClient


class Interface(object):

    def __init__(self):
        self.conn = MySQLdb.connect(host='202.119.84.47', user='root', passwd='qwert123456', db='weibo_test', port=3306)
        self.cur = self.conn.cursor()
        client = MongoClient('localhost', 27017)
        db = client['weibo']
        self.col = db['burst_events']
        self.col_track = db['tracking_events']

    # generate json
    def get_now_events(self, n_time, p_time):
        # burst_events = col.find({'timestamp': {'$lt': '2015-06-20 20:50', '$gt': '2015-06-20 19:00'}})\
        #         .sort([('timestamp', pymongo.ASCENDING), ('burst_tweets_count', pymongo.DESCENDING)])
        burst_events = self.col.find({'timestamp': {'$gte': p_time, '$lt': n_time}})\
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
        # s_time = '2015-06-20 19:00'
        # e_time = '2015-06-20 20:50'
        burst_events = self.col.find({'timestamp': {'$lt': e_time, '$gte': s_time}})\
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

    def get_events_by_parentid(self, _id):
        event = self.col_track.find_one({'_id': bson.objectid.ObjectId(_id)})
        object_ids = event['burst_events_objectid']
        events = dict()
        for i in xrange(len(object_ids)):
            event = self.col.find_one({'_id': object_ids[i]})
            event['_id'] = str(event['_id'])
            event['parent_id'] = str(event['parent_id'])
            events[i + 1] = event
        return json.dumps(events)

    def get_single_event(self, _id):
        event = self.col.find_one({'_id': bson.objectid.ObjectId(_id)})
        event['_id'] = str(event['_id'])
        event['parent_id'] = str(event['parent_id'])
        event = json.dumps(event)
        return event

    def keep_mysql_alive(self):
        try:
            self.cur.execute('select id from tweers limit 1')
        except:
            print 'lost server...,reconnect...'
            self.conn = MySQLdb.connect(host='202.119.84.47', user='root', passwd='qwert123456', db='weibo_test', port=3306)
            self.cur = self.conn.cursor()

    def get_tweets_by_eventid(self, _id):
        self.keep_mysql_alive()
        event = self.col.find_one({'_id': bson.objectid.ObjectId(_id)})
        tweets_id = event['tweets_id']
        events = dict()
        for i in xrange(len(tweets_id)):
            events[i + 1] = self.get_single_tweet(tweets_id[i])

        return json.dumps(events)

    def get_single_tweet(self, _id):
        event = dict()
        sql = "select id, username, timestamp, pageurl, location, is_v, \
                score, raw_content, pos, neu, neg from tweets where id=%s"
        self.cur.execute(sql, (_id, ))
        try:
            _id, username, timestamp, pageurl, location, is_v, score, content, pos, neu, neg = self.cur.fetchone()
        except:
            print 'error'
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
            return event

    def get_single_event_detail(self, _id):
        self.keep_mysql_alive()
        return json.dumps(self.get_single_tweet(_id))


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
