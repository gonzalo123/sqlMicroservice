from flask import Flask, g, jsonify, request, abort, after_this_request
from jinja2 import Environment, PackageLoader, TemplateNotFound
from cStringIO import StringIO as IO
from flask.json import JSONEncoder
from datetime import datetime
import psycopg2.extras
import functools
import psycopg2
import gzip

CONF = dict(
    DATABASE="dbname=gonzalo user=username",
    DEBUG=True,
    VALID_TOKEN="superSecretToken"
)

class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, datetime):
                return obj.strftime('%d/%m/%Y %H:%M:%S')
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)

def isDate(type):
    # types https://github.com/psycopg/psycopg2/blob/master/psycopg/pgtypes.h
    return type in (1114,)

def isValidToken(access_token):
    return CONF['VALID_TOKEN'] == access_token

def authorized(fn):
    def _wrap(*args, **kwargs):
        if 'Authorization' not in request.headers and '_authToken' not in request.args:
            abort(401)
            return None

        if 'Authorization' in request.headers:
            token = request.headers['Authorization']

        if '_authToken' in request.args:
            token = request.args['_authToken']

        if not isValidToken(token):
            abort(401)
            return None

        return fn(*args, **kwargs)
    return _wrap

def gzipped(f):
    @functools.wraps(f)
    def view_func(*args, **kwargs):
        @after_this_request
        def zipper(response):
            accept_encoding = request.headers.get('Accept-Encoding', '')

            if 'gzip' not in accept_encoding.lower():
                return response
            response.direct_passthrough = False

            if (response.status_code < 200 or response.status_code >= 300 or 'Content-Encoding' in response.headers):
                return response

            gzip_buffer = IO()
            gzip_file = gzip.GzipFile(mode='wb', fileobj=gzip_buffer)
            gzip_file.write(response.data)
            gzip_file.close()

            response.data = gzip_buffer.getvalue()
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Vary'] = 'Accept-Encoding'
            response.headers['Content-Length'] = len(response.data)
            return response

        return f(*args, **kwargs)

    return view_func

app = Flask(__name__)
app.json_encoder = CustomJSONEncoder

@app.errorhandler(TemplateNotFound)
def not_found_exception_handler(error):
    return 'Not found', 404

app.config.update(CONF)

@app.before_request
def before_request():
    g.env = Environment(loader=PackageLoader('sqlStorage', 'sql'))
    g.db = psycopg2.connect(app.config['DATABASE'])

@app.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()

@app.route('/sql/<string:path>', methods=['GET'])
@authorized
@gzipped
def do(path):
    templatePath = path.replace(".", "/") + ".sql"
    template = g.env.get_template(templatePath)
    params = request.args if len(request.args) > 0 else {}
    cursorFactory = psycopg2.extras.NamedTupleCursor if '_assoc' in request.args else None
    cursorName = templatePath if '_cursor' in request.args else None

    sql = template.render(params)
    conn = g.db

    cursor = conn.cursor(name=cursorName, cursor_factory=cursorFactory)

    if '_itersize' in request.args:
        cursor.itersize = request.args['_itersize']

    cursor.execute(sql, params)

    data = cursor.fetchall()
    coluns = []

    for desc in cursor.description:
        coluns.append({'name': desc[0], 'typeCode': desc[1], 'isDate': isDate(desc[1])})

    metadata = {
        'count': len(data),
        'columns': coluns
    }
    cursor.close()

    return jsonify(metadata=metadata, data=data)

if __name__ == "__main__":
    app.run()
