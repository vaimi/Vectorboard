from werkzeug.serving import run_simple
from werkzeug.wsgi import DispatcherMiddleware
from vectorsite.application import app as vector_site
from api.dp import server as api

application = DispatcherMiddleware(api, {
    '/vectorsite/client': vector_site
})
if __name__ == '__main__':
    vector_site.debug = True
    api.debug = True
    run_simple('localhost', 5000, application,
               use_reloader=True, use_debugger=True, use_evalex=True)
