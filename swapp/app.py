import os.path
from concurrent.futures import ThreadPoolExecutor

from flask import Flask

from werkzeug.wsgi import DispatcherMiddleware

from hiku.engine import Engine
from hiku.console.ui import ConsoleApplication
from hiku.typedef.kinko import dumps as dump_types
from hiku.readers.simple import read
from hiku.executors.threads import ThreadsExecutor

from kinko.lookup import Lookup
from kinko.loaders import FileSystemLoader
from kinko.typedef import load_types

from .model import setup, session as sa_session
from .graph import GRAPH


sa_engine = setup()
sa_session.configure(bind=sa_engine)

thread_pool = ThreadPoolExecutor(2)
engine = Engine(ThreadsExecutor(thread_pool))

ui_path = os.path.join(os.path.dirname(__file__), 'ui')
lookup = Lookup(load_types(dump_types(GRAPH)), FileSystemLoader(ui_path))

app = Flask(__name__)


@app.route('/')
def index():
    fn = lookup.get('index/view')
    print('QUERY:', fn.query())
    hiku_query = read(repr(fn.query()))
    result = engine.execute(GRAPH, hiku_query)
    print('RESULT:', result)
    return fn.render(result)


app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    '/graph': ConsoleApplication(GRAPH, engine),
})
