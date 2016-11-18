import os.path
from pprint import pprint
from concurrent.futures import ThreadPoolExecutor

from flask import Flask

from werkzeug.wsgi import DispatcherMiddleware

from hiku.engine import Engine
from hiku.result import denormalize
from hiku.console.ui import ConsoleApplication
from hiku.typedef.kinko import dumps as dump_types
from hiku.readers.simple import read
from hiku.executors.threads import ThreadsExecutor

from kinko.lookup import Lookup
from kinko.loaders import FileSystemLoader
from kinko.typedef import load_types

from .model import setup
from .graph import GRAPH, SA_ENGINE_KEY


sa_engine = setup()

hiku_engine_ctx = {SA_ENGINE_KEY: sa_engine}

thread_pool = ThreadPoolExecutor(2)
engine = Engine(ThreadsExecutor(thread_pool))

ui_path = os.path.join(os.path.dirname(__file__), 'ui')
lookup = Lookup(load_types(dump_types(GRAPH)), FileSystemLoader(ui_path))

app = Flask(__name__)


@app.route('/')
def index():
    fn = lookup.get('index/view')
    query_str = str(fn.query())
    print('QUERY:', query_str)

    hiku_query = read(query_str)
    result = engine.execute(GRAPH, hiku_query,
                            ctx=hiku_engine_ctx)
    print('RESULT:')
    pprint(denormalize(GRAPH, result, hiku_query))
    return fn.render(result)


app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/graph': ConsoleApplication(GRAPH, engine, ctx=hiku_engine_ctx,
                                 debug=True),
})
