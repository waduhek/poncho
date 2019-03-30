from http import server
import socketserver

PORT = 8000

Handler = server.SimpleHTTPRequestHandler

httpd = socketserver.TCPServer(("", PORT), Handler)

print("serving at port ", PORT)
httpd.serve_forever()