import os
import sys

import tornado.ioloop
import tornado.web

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from nmt_chatbot.inference import inference


class MainHandler2(tornado.web.RequestHandler):
    def get(self):
        self.render('chatbot.html')


class MainHandler(tornado.web.RequestHandler):
    def post(self):
        data = self.request.body
        # Call your function here and save it into output
        output = inference(data.decode('utf-8').split('=')[1])['answers'][0]
        self.write(output)


def make_app():
    return tornado.web.Application(
        [(r"/msg", MainHandler), (r"/", MainHandler2)],
        template_path=os.path.join(os.path.dirname(__file__), "templates"),
        debug=True
    )


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()