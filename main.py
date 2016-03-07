#!/usr/bin/env python
# encoding: utf-8


from datetime import datetime
import os.path

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

from test import Interface

from tornado.options import define, options
define("port", default=8888, help="run on the give port", type=int)

inter = Interface()

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello world!")

class GetNowEventsHandler(tornado.web.RequestHandler):
    def get(self, n_time):
        nn_time = datetime.fromtimestamp(int(n_time)).strftime('%Y-%m-%d %H:%M')
        pre_time = datetime.fromtimestamp(int(n_time) - 600).strftime('%Y-%m-%d %H:%M')
        events = inter.get_now_events(nn_time, pre_time)
        self.write(events)

class GetPreEventsHandler(tornado.web.RequestHandler):
    def get(self, s_time, e_time):
        s_time = datetime.fromtimestamp(int(s_time)).strftime('%Y-%m-%d %H:%M')
        e_time = datetime.fromtimestamp(int(e_time)).strftime('%Y-%m-%d %H:%M')
        events = inter.get_pre_events(s_time, e_time)
        self.write(events)

class GetTrackEventsHandler(tornado.web.RequestHandler):
    def get(self, _id):
        events = inter.get_tracking_events(_id)
        self.write(events)

class GetSingleEventHandler(tornado.web.RequestHandler):
    def get(self, _id):
        event = inter.get_single_event(_id)
        self.write(event)

class GetSingleEventDetailHandler(tornado.web.RequestHandler):
    def get(self, _id):
        event = inter.get_single_event_detail(_id)
        self.write(event)


if __name__ == '__main__':
    tornado.options.parse_command_line()
    app = tornado.web.Application(
        handlers=[(r'/', IndexHandler),
            (r'/get_now_events/(\w+)', GetNowEventsHandler),
            (r'/get_pre_events/(\w+)/(\w+)', GetPreEventsHandler),
            (r'/get_track_events/(\w+)', GetTrackEventsHandler),
            (r'/get_single_event/(\w+)', GetSingleEventHandler),
            (r'/get_single_event_detail/(\w+)', GetSingleEventDetailHandler)
            ],
        debug=True
    )
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()


