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
import socket
import logging
import json

log = logging.getLogger('web_server')
log.setLevel(logging.INFO)
fh = logging.FileHandler('./logs/sensor_vs_time.log')
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
        # Doesn't do anything with posted data
        content_length = int(self.headers['Content-Length'])  # <--- Gets the size of data
        post_data = self.rfile.read(content_length)  # <--- Gets the data itself
        content = json.loads(str(post_data, 'utf8'))

        streamer_id = content['event']['correlationData']['event_id']
        log.info('Notification | ' + streamer_id)
        print('Notification from:', streamer_id)
        self._set_headers()
        # self.wfile.write(post_data)


def run(server_class=HTTPServer, handler_class=S, port=80):
    host_ip = socket.gethostbyname(socket.gethostname())
    server_address = (host_ip, port)
    httpd = server_class(server_address, handler_class)
    print('Starting httpd...')
    print('Host ip:', host_ip)
    httpd.serve_forever()


if __name__ == "__main__":
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
