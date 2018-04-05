#!/usr/bin/env python
"""
Very simple HTTP server in python.

Usage::
    ./dummy-web-server.py [<port>]

Send a GET request::
    curl http://localhost

Send a HEAD request::
    curl -I http://localhost

Send a POST request::
    curl -d "foo=bar&bin=baz" http://localhost

"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import socket, threading, time
import logging
import json

log = logging.getLogger('web_server')
log.setLevel(logging.INFO)
fh = logging.FileHandler('./logs/sensor_vs_time_not.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(created)f | %(message)s')
fh.setFormatter(formatter)
log.addHandler(fh)


class S(BaseHTTPRequestHandler):

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        self.wfile.write(b'{"response": {"value": "Hi!", "request": "GET"}}')

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        global counter
        self._set_headers()
        # Doesn't do anything with posted data
        content_length = int(self.headers['Content-Length'])  # <--- Gets the size of data
        post_data = self.rfile.read(content_length)  # <--- Gets the data itself
        content = json.loads(str(post_data, 'utf8'))

        streamer_id = content['event']['correlationData']['event_id']
        log.info('Notification ' + str(counter) + ' | 300 | ' + streamer_id)
        # print('Notification from:', streamer_id)
        print('count: ', counter)
        counter += 1

        # self.wfile.write(post_data)

# Create ONE socket
host_ip = socket.gethostbyname(socket.gethostname())
addr = (host_ip, 9090)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(addr)
sock.listen(50)

# Launch 100 listener threads
class Thread(threading.Thread):
    def __init__(self, i):
        threading.Thread.__init__(self)
        self.i = i
        self.deamon = True
        self.start()

    def run(self):
        httpd = HTTPServer(addr, S, False)

        # Prevent the HTTP server from re-binding every handler.
        httpd.socket = sock
        httpd.server_bind = self.server_close = lambda self: None
        httpd.serve_forever()


if __name__ == "__main__":
    threads = 200
    counter = 0
    [Thread(i) for i in range(threads)]
    print('Starting httpd with %s  threads...' % threads)
    print('Host ip:', host_ip)
    time.sleep(1e9)
