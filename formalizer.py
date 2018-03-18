"""
Project: 
Author: ManuelG
Created: 29-Nov-17 14:32
License: MIT
"""

import os
import redis
import json
import urllib.parse
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.wsgi import SharedDataMiddleware
from werkzeug.utils import redirect
from jinja2 import Environment, FileSystemLoader


# validate JSON formal

def is_valid_event(event):
    """
    Validades if event is a valid JSON
    :param event:
    :return:
    """
    try:
        json.loads(event)
    except ValueError:
        return False
    return True

# Application


class Formalizer(object):

    def __init__(self, config):
        #configure database
        self.redis = redis.Redis(config['redis_host'], config['redis_port'])
        self.url_map = Map([
            Rule('/g-event', endpoint='new_event')
        ])
        template_path = os.path.join(os.path.dirname(__file__), 'templates')
        self.jinja_env = Environment(loader=FileSystemLoader(template_path), autoescape=True)

    def render_template(self, template_name, **context):
        t = self.jinja_env.get_template(template_name)
        return Response(t.render(context), mimetype='text/html')

    # View 1
    def on_new_event(self, request):
        error = None
        event = ''
        if request.method == 'POST':
            event = request.form['event']
            if not is_valid_event(event):
                error = 'The event definition is not a valid JSON'
            else:
                pass
        else:
            return self.render_template('new_event.html', error=error, url=event)

    def dispatch_request(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, 'on_' + endpoint)(request, **values)
        except HTTPException as e:
            return e

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)


def create_app(redis_host='localhost', redis_port=6379, with_static=True):
    app = Formalizer({
        'redis_host': redis_host,
        'redis_port': redis_port
    })
    if with_static:
        app.wsgi_app = SharedDataMiddleware(app.wsgi_app, {'/static': os.path.join(os.path.dirname(__file__), 'static')})
    return app

if __name__ == '__main__':
    from werkzeug.serving import run_simple
    app = create_app(with_static=False)
    run_simple('127.0.0.1', 5000, app, use_debugger=True, use_reloader=True)
