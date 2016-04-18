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

    def get_detection_event_by_id(self, _id):
        '''
        根据_id获取检测事件
        '''
        event = self.col.find_one({'_id': bson.objectid.ObjectId(_id)})
        event['_id'] = str(event['_id'])
        event['parent_id'] = str(event['parent_id'])
        event = json.dumps(event)
        return event

    def get_detection_events_by_time(self, s_time, e_time):
        """
        获取时间间隔内的检测事件
        """
        burst_events = self.col.find({'timestamp': {'$gt': s_time, '$lte': e_time}})\
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

    def get_tracking_event_by_id(self, _id):
        '''
        根据_id获取跟踪事件
        '''
        event = self.col_track.find_one({'_id': bson.objectid.ObjectId(_id)})
        object_ids = event['burst_events_objectid']
        for i in xrange(len(object_ids)):
            object_ids[i] = str(object_ids[i])
        event['burst_events_objectid'] = object_ids
        event['_id'] = str(event['_id'])
        event = json.dumps(event)
        return event

    def get_tracking_events_by_time(self, s_time, e_time):
        '''
        获取时间间隔内的跟踪事件
        '''
        # s_time = '2015-06-20 19:00'
        # e_time = '2015-06-20 20:50'
        burst_events = self.col_track.find({'timestamp': {'$lte': e_time, '$gt': s_time}})\
            .sort([('timestamp', pymongo.ASCENDING), ('burst_tweets_count', pymongo.DESCENDING)])
        events = dict()
        cnt = 1
        for event in burst_events:
            event['_id'] = str(event['_id'])
            events[str(cnt)] = event
            object_ids = event['burst_events_objectid']
            for i in xrange(len(object_ids)):
                object_ids[i] = str(object_ids[i])
            event['burst_events_objectid'] = object_ids
            cnt += 1

        events = json.dumps(events)
        return events

    def get_events_by_parentid(self, _id):
        '''
        根据跟踪事件id获取此事件下所有的检测事件
        '''
        event = self.col_track.find_one({'_id': bson.objectid.ObjectId(_id)})
        object_ids = event['burst_events_objectid']
        events = dict()
        for i in xrange(len(object_ids)):
            event = self.col.find_one({'_id': object_ids[i]})
            event['_id'] = str(event['_id'])
            event['parent_id'] = str(event['parent_id'])
            events[i + 1] = event
        return json.dumps(events)

    def _keep_mysql_alive(self):
        try:
            self.cur.execute('select id from tweets limit 1')
        except:
            print 'lost server...,reconnect...'
            self.conn = MySQLdb.connect(host='202.119.84.47', user='root', passwd='qwert123456', db='weibo_test', port=3306)
            self.cur = self.conn.cursor()

    def get_single_tweet_by_id(self, _id):
        self._keep_mysql_alive()
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

    def _get_tweets_by_eventid(self, _id):
        event = self.col.find_one({'_id': bson.objectid.ObjectId(_id)})
        tweets_id = event['tweets_id']
        events = dict()
        # for i in xrange(len(tweets_id)):
        #     events[i + 1] = self.get_single_tweet(tweets_id[i])
        sql = "select id, username, timestamp, pageurl, location, is_v, \
                score, raw_content, pos, neu, neg from tweets where id in %s"
        self.cur.execute(sql, (tweets_id, ))
        try:
            results = self.cur.fetchall()
            for i in xrange(len(results)):
                _id, username, timestamp, pageurl, location, is_v, score, content, pos, neu, neg = results[i]
                event = dict()
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
                events[i + 1] = event
        except:
            print 'Error'
        return events

    def get_tweets_by_trackid(self, _id):
        self._keep_mysql_alive()
        eventids = self.col_track.find_one({'_id': bson.objectid.ObjectId(_id)})['burst_events_objectid']
        events = dict()
        for eventid in eventids:
            events[str(eventid)] = self._get_tweets_by_eventid(str(eventid))
        return json.dumps(events)

    def get_tweets_by_eventid(self, _id):
        self._keep_mysql_alive()
        return json.dumps(self._get_tweets_by_eventid(_id))
        '''
        event = self.col.find_one({'_id': bson.objectid.ObjectId(_id)})
        tweets_id = event['tweets_id']
        events = dict()
        # for i in xrange(len(tweets_id)):
        #     events[i + 1] = self.get_single_tweet(tweets_id[i])
        sql = "select id, username, timestamp, pageurl, location, is_v, \
                score, raw_content, pos, neu, neg from tweets where id in %s"
        self.cur.execute(sql, (tweets_id, ))
        try:
            results = self.cur.fetchall()
            for i in xrange(len(results)):
                _id, username, timestamp, pageurl, location, is_v, score, content, pos, neu, neg = results[i]
                event = dict()
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
                events[i + 1] = event
        except:
            print 'Error'

        return json.dumps(events)
        '''

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
