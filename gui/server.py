import tornado.ioloop
import tornado.web
import os
class MainHandler2(tornado.web.RequestHandler):
    def get(self):
        self.render('chatbot.html')


class MainHandler(tornado.web.RequestHandler):
    def post(self):
        data=self.request.body
        print(data)
        #print(self.request)
        self.write("abc");

def make_app():
    return tornado.web.Application([(r"/msg", MainHandler),(r"/", MainHandler2)],template_path = os.path.join(os.path.dirname(__file__),"templates"))

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()