#!/usr/bin/env python
# encoding: utf-8


from datetime import datetime
import os.path

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from utils import Interface

from tornado.options import define, options
define("port", default=8888, help="run on the give port", type=int)

inter = Interface()

def simulate_time(ntime):
    ntime = datetime.fromtimestamp(int(ntime)).strftime('%Y-%m-%d %H:%M')
    ntime = ntime.split('-')
    ntime[0] = '2015'
    ntime[1] = '06'
    return '-'.join(ntime)

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello world!")

class GetDetectionEventByIDHandler(tornado.web.RequestHandler):
    def get(self, _id):
        event = inter.get_detection_event_by_id(_id)
        self.write(event)

class GetDetectionEventsByTimeHandler(tornado.web.RequestHandler):
    def get(self, n_time):
        # nn_time = datetime.fromtimestamp(int(n_time)).strftime('%Y-%m-%d %H:%M')
        # pre_time = datetime.fromtimestamp(int(n_time) - 600).strftime('%Y-%m-%d %H:%M')
        nn_time = simulate_time(n_time)
        pre_time = simulate_time(int(n_time) - 600)
        events = inter.get_detection_events_by_time(pre_time, nn_time)
        self.write(events)

class GetTrackingEventByIDHandler(tornado.web.RequestHandler):
    def get(self, _id):
        event = inter.get_tracking_event_by_id(_id)
        self.write(event)

class GetTrackingEventsByTimeHandler(tornado.web.RequestHandler):
    def get(self, s_time, e_time):
        # s_time = datetime.fromtimestamp(int(s_time)).strftime('%Y-%m-%d %H:%M')
        # e_time = datetime.fromtimestamp(int(e_time)).strftime('%Y-%m-%d %H:%M')
        s_time = simulate_time(s_time)
        e_time = simulate_time(e_time)
        events = inter.get_tracking_events_by_time(s_time, e_time)
        self.write(events)

class GetSingleTweetByIDHandler(tornado.web.RequestHandler):
    def get(self, _id):
        tweet = inter.get_single_tweet_by_id(_id)
        self.write(tweet)

class GetTweetsByEventIDHandler(tornado.web.RequestHandler):
    def get(self, _id):
        tweets = inter.get_tweets_by_eventid(_id)
        self.write(tweets)

class GetTweetsByTrackIDHandler(tornado.web.RequestHandler):
    def get(self, _id):
        tweets = inter.get_tweets_by_trackid(_id)
        self.write(tweets)

class GetEventsByParentIDHandler(tornado.web.RequestHandler):
    def get(self, _id):
        events = inter.get_events_by_parentid(_id)
        self.write(events)

if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = tornado.web.Application(
        handlers=[(r'/', IndexHandler),
            (r'/get_detection_event_by_id/(\w+)', GetDetectionEventByIDHandler),
            (r'/get_detection_events_by_time/(\w+)', GetDetectionEventsByTimeHandler),
            (r'/get_tracking_event_by_id/(\w+)', GetTrackingEventByIDHandler),
            (r'/get_tracking_events_by_time/(\w+)/(\w+)', GetTrackingEventsByTimeHandler),
            (r'/get_single_tweet_by_id/(\w+)', GetSingleTweetByIDHandler),
            (r'/get_tweets_by_eventid/(\w+)', GetTweetsByEventIDHandler),
            (r'/get_tweets_by_trackid/(\w+)', GetTweetsByTrackIDHandler),
            (r'/get_events_by_parentid/(\w+)', GetEventsByParentIDHandler)
            ],
        debug=True
    )
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()


