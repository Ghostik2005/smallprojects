# coding: utf-8

__appname__ = "xmltojson"
__version__ = "17.191.1220" #добавлена обработка POST запроса
#__version__ = "17.181.1520" #первая редакция
__profile__ = ""
__index__   =-1


import sys
sys.PY3 = sys.version_info[0] > 2
if not sys.PY3:
    input = raw_input
import os, time
import traceback

import socket
__hostname__ = socket.gethostname().lower()

import threading, random, subprocess
if sys.PY3:
    from urllib.parse import unquote
else:
    from urllib import unquote

import uuid
import glob
import gzip
import shutil
import hashlib

from lockfile import LockWait

APPCONF = {
    "params": [],
    "kwargs": {},
    #"hostname": socket.gethostname().lower(),
    "addr": ("127.0.0.1", 0),
    "nginx": {
        "location": "/ms71/conf/location",
        "upstream": "/ms71/conf/upstream",
    },
}

def main():
    rc = 0
    try:
        APPCONF["params"], APPCONF["kwargs"] = handle_commandline()
        APPCONF["addr"] = APPCONF["kwargs"].pop("addr", APPCONF["addr"])
        serve_forever(APPCONF["addr"], application, init)
    except KeyboardInterrupt as e:
        pass
    except SystemExit as e:
        if e:
            rc = e.code
    except:
        log(traceback.format_exc(), kind="error")
    finally:
        try:
            finit()
        except:
            log(traceback.format_exc(), kind="error:finit")
    os._exit(rc)

def application(env):
    """
CONTENT_ENCODING = <class 'str'> gzip
CONTENT_LENGTH = <class 'int'> 5421
CONTENT_TYPE = <class 'str'> application/json
HTTP_KWARGS = <class 'dict'> {}
HTTP_METHOD = <class 'str'> POST
HTTP_PARAMS = <class 'list'> []
REMOTE_ADDR = <class 'str'> 79.104.1.86
REMOTE_PORT = <class 'str'> 65083
ROOT = <class 'str'> /usr/share/nginx/html
SCGI = <class 'str'> 1
SERVER_NAME = <class 'str'> online365.pro
SERVER_PORT = <class 'str'> 443
URI = <class 'str'> /sgg/
X-API-KEY = <class 'str'> 80a3fd3ba997493f837894f1af803216
X-BODY-FILE = <class 'str'> /usr/share/nginx/temp/0000000005
scgi.defer = <class 'NoneType'> None
scgi.initv = <class 'list'> [('127.0.0.1', 50703), 6113]
scgi.rfile = <class '_io.BufferedReader'> <_io.BufferedReader name=5>
scgi.wfile = <class 'socket.SocketIO'> <socket.SocketIO object at 0x7f90edc44240>
"""
    from lib.xmltojson import main as main2
    import itertools
    import zlib
    addr, pid = env["scgi.initv"][:2]
    msg = f'{addr[0]} {addr[1]} {env["HTTP_METHOD"]} {env["URI"]} {env["HTTP_PARAMS"]} {env["HTTP_KWARGS"]}'
    env["scgi.defer"] = lambda: log("%s close" % msg)
    log(msg)
    #добавляем чтоб постом можно было пеедавать строку для конвертации
    content = u"""Манускрипт.Онлайн

Сервис XMLtoJSON преобразует XML документ в JSON 

Пример использования (метод GET):
  http://online365.pro/xmltojson?/ms71/data/filename.xml - локальный файл
  http://online365.pro/xmltojson?http://www.hostname.org/qqwtrtr7646gfyjy.xml - url файла

Пример использования (метод POST):
  http://online365.pro/xmltojson
  в теле запроса - xml строка
  методо сжатия deflate
  возвращает сроку JSON
""".encode()
    fg_head = True
    data = None
    _rm = env["HTTP_METHOD"].upper()
    arg=None
    if 'GET' == _rm:
        _qs = env["HTTP_PARAMS"]
        g = (v.strip() for v in itertools.chain.from_iterable(item.split(',') for item in _qs))
        args = list(filter(lambda x: x, g))
        try:
            arg = args.pop(0)
        except Exception as Err:
            #print(Err)
            pass
    elif 'POST' == _rm:
        arg = env['scgi.rfile'].read(env['CONTENT_LENGTH'])
        try:
            arg = zlib.decompress(arg)
        except Exception as Err:
            #log(Err)
            pass
    if arg:
        fg_head = False
        data = main2(arg)

    header = head(len(content), False, True)
    if not fg_head:
        if data:
            content = data.encode()
            header = head(len(content), True, False)
        else:
            content = u"something wrong".encode()
            header = head(len(content), False, True)

    # три обязательных вызова yield: статус, заголовки, содержание
    yield '200 OK'
    yield header
    yield content

def head(aContentLength, fgDeflate=True, fg_head=False):
    aLastModified = time.strftime('%a, %d %b %Y %X GMT', time.gmtime())
    r = []
    r.append(("Last-Modified", "%s" % aLastModified))
    r.append(("Content-Length", "%i" % aContentLength))
    r.append(("X-Accel-Buffering", "no"))
    if fg_head:
        r.append(("Cache-Control", "no-cache"))
        r.append(("Content-Type", "text/plain; charset=UTF-8"))
    else:
        r.append(("Content-Disposition", "attachment; filename=document.json"))
        r.append(("Content-Type", "application/octet-stream"))
    if fgDeflate:
        r.append(("Content-Encoding", "deflate"))
    return r

    

def init(sock):
    addr = sock.getsockname()[:2]
    sock.listen(100)
    APPCONF["addr"] = addr
    fileupstream = _getfilename("upstream")
    APPCONF["fileupstream"] = fileupstream

    data = """location /xmltojson {
    limit_except GET POST{
        deny all;
    }
    include scgi_params;
    scgi_param                X-BODY-FILE $request_body_file;
    scgi_param                X-API-KEY $http_x_api_key;
    scgi_pass xmltojson_scgi;
    scgi_buffering            off;
    scgi_cache                off;
}
"""
    filelocation = _getfilename("location")
    dn = os.path.dirname(filelocation)
    bs = os.path.basename(filelocation)
    _filelocation = os.path.join(dn, bs.split('.', 1)[0].split('-', 1)[0])  # общий файл для всех экземпляров приложения
    with open(_filelocation, "wb") as f:
        f.write(data.encode())
    APPCONF["filelocation"] = _filelocation
    dn = os.path.dirname(fileupstream)
    bs = os.path.basename(fileupstream)
    _fileupstream = os.path.join(dn, bs.split('.', 1)[0].split('-', 1)[0])  # общий файл для всех экземпляров приложения
    _fileupstreamlock = bs.split('.', 1)[0].split('-', 1)[0]  # _fileupstream + '.lock'
    data1 = """upstream xmltojson_scgi { least_conn;
    server %s:%s;  # %s
}
""" % (addr[0], addr[1], bs)
    data2 = """#   server %s:%s;  # %s""" % (addr[0], addr[1], bs)
    with LockWait(_fileupstreamlock):
        if os.path.exists(_fileupstream):
            with open(_fileupstream, "rb") as f:
                src = f.read().decode().rstrip().splitlines()
                # + ' ' + data[1:] + '\n}\n'
            _find = "# %s" % bs
            # fg - пердполагаем, что надо добавлять свой апстрим
            fg = True
            for i in range(1, len(src)-1):
                if src[i].find(_find) >-1:
                    fg = False
                    src[i] = ' ' + data2[1:]
                    break
            if fg:
                src[len(src)-1] = ' ' + data2[1:] + '\n}\n'
            src = '\n'.join(src)
            with open(_fileupstream, "wb") as f:
                f.write(src.encode())
        else:
            with open(_fileupstream, "wb") as f:
                f.write(data1.encode())
    rc = 0
    rc = subprocess.call(['sudo', 'nginx', '-t', '-c', '/ms71/saas.conf', '-p', '/ms71/'])
    if 0 == rc:
        rc = subprocess.call(['sudo', 'nginx', '-s', 'reload', '-c', '/ms71/saas.conf', '-p', '/ms71/'])
        if 0 == rc:
            log("%s:%s running" % addr)
            return [addr, os.getpid()]
    raise SystemExit(rc)

def _getfilename(name):
    filename = ""
    if __index__ > -1:
        if __profile__:
            filename = os.path.join(APPCONF["nginx"][name], "%s-%s.%s" % (__appname__, __index__, __profile__))
        else:
            filename = os.path.join(APPCONF["nginx"][name], "%s-%s" % (__appname__, __index__))
    else:
        if __profile__:
            filename = os.path.join(APPCONF["nginx"][name], "%s.%s" % (__appname__, __profile__))
        else:
            filename = os.path.join(APPCONF["nginx"][name], __appname__)
    return filename

def finit():
    fileupstream = APPCONF.get("fileupstream")
    if fileupstream is None:
        log("%s:%s critical" % APPCONF["addr"], begin='')
        return
    try:
        os.remove(fileupstream)
    except: pass
    dn = os.path.dirname(fileupstream)
    bs = os.path.basename(fileupstream)
    _fileupstream = os.path.join(dn, bs.split('.', 1)[0].split('-', 1)[0])
    _fileupstreamlock = bs.split('.', 1)[0].split('-', 1)[0]  # _fileupstream + '.lock'
    with LockWait(_fileupstreamlock):
        _find = "# %s" % bs
        src = ""
        fg_noapp = True
        if os.path.exists(_fileupstream):
            with open(_fileupstream, "rb") as f:
                src = f.read().decode().rstrip().splitlines()
            for i in range(1, len(src)-1):
                if src[i].find(_find) >-1:
                    src.pop(i)
                    break
            fg_noapp = 0 == len(src[1:-1])
        if fg_noapp:  # нет запущенных приложений, удаляем общую локацию и апстрим
            try:
                os.remove(APPCONF["filelocation"])
            except: pass
            try:
                os.remove(_fileupstream)
            except: pass
        else:
            src = '\n'.join(src)
            with open(_fileupstream, "wb") as f:
                f.write(src.encode())
    subprocess.call(['sudo', 'nginx', '-s', 'reload', '-c', '/ms71/saas.conf', '-p', '/ms71/'])
    log("%s:%s shutdown" % APPCONF["addr"], begin='')


def serve_forever(addr, handle_request, init=None):
    sock = None
    if type(addr) is str:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    else:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(addr)
    #sock.listen(10)
    initial_value = None
    if init:
        if callable(init):
            initial_value = init(sock)
        else:
            initial_value = init
    try:
        while True:
            _conn, _addr = sock.accept()
            _t = threading.Thread(target=_handle_conn, args=(_conn, _addr, handle_request, initial_value))
            _t.env = None
            _t.daemon = True
            _t.start()
    finally:
        try: sock.close()
        except: pass


def _handle_conn(conn, addr, handle_request, initial_value):
    env = None
    try:
        conn.settimeout(1)
        rfile = conn.makefile("rb", -1)
        wfile = conn.makefile("wb", 0)

        env = _env_read(rfile)
        env = _args_parse(env)
        #env["scgi.connection"] = conn
        #env["scgi.address"] = addr
        env["scgi.defer"] = None
        env["scgi.initv"] = initial_value
        env["scgi.rfile"] = rfile
        env["scgi.wfile"] = wfile
        env["CONTENT_LENGTH"] = int(env["CONTENT_LENGTH"])
        threading.current_thread().env = env

        g = handle_request(env)

        wfile.write("Status: {0}\r\n".format(g.__next__()).encode())
        wfile.flush()
        for kv in g.__next__():
            wfile.write(": ".join(kv).encode())
            wfile.write(b"\r\n")
        wfile.write(b"\r\n")
        wfile.flush()

        for data in g:
            wfile.write(data)
            wfile.flush()
    except (BrokenPipeError) as e:
        pass
    except:
        print(conn, file=sys.stderr)
        print(env, file=sys.stderr)
        traceback.print_exc()
    finally:
        if not wfile.closed:
            try: wfile.flush()
            except: pass
        try: wfile.close()
        except: pass
        try: rfile.close()
        except: pass
        try: conn.shutdown(socket.SHUT_WR)
        except: pass
        try: conn.close()
        except: pass
        if env and env.get("scgi.defer"):
            try:
                env["scgi.defer"]()
            except:
                log(traceback.format_exc(), kind="error:defer")

# netstring utility functions
def _env_read(f):
    size, d = f.read(16).split(b':', 1)
    size = int(size)-len(d)
    if size > 0:
        s = f.read(size)
        if not s:
            raise IOError('short netstring read')
        if f.read(1) != b',':
            raise IOError('missing netstring terminator')
        items =  b"".join([d, s]).split(b'\0')[:-1]
    else:
        raise IOError('missing netstring size')
    assert len(items) % 2 == 0, "malformed headers"
    env = {}
    while items:
        v = items.pop()
        k = items.pop()
        env[k.decode()] = v.decode()
    return env

def _args_parse(env):
    args = []
    argd = {}
    for x in env.pop('ARGS', '').split('&'):
        i = x.find('=')
        if i > -1:
            k, x  = x[:i], x[i+1:]
        else:
            k = None
        if k:
            argd[unquote(k)] = unquote(x)
            #argd[k] = x
        else:
            if x:
                args.append(unquote(x))
                #args.append(x)
    env['HTTP_PARAMS'] = args
    env['HTTP_KWARGS'] = argd
    return env

def log(msg, kind='info', begin='', end='\n'):
    try:
        ts = "%Y-%m-%d %H:%M:%S"
        try: ts = time.strftime(ts)
        except: ts = time.strftime(ts)
        if __hostname__:
            if __profile__:
                s = '{0}{1} {2} {4}.{5}:{3}:{6} {7}{8}'.format(begin, ts, __hostname__, __version__, __appname__, __profile__, kind, msg, end)
            else:
                s = '{0}{1} {2} {4}:{3}:{5} {6}{7}'.format(begin, ts, __hostname__, __version__, __appname__, kind, msg, end)
        else:
            if __profile__:
                s = '{0}{1} {3}.{4}:{2}:{5} {6}{7}'.format(begin, ts, __version__, __appname__, __profile__, kind, msg, end)
            else:
                s = '{0}{1} {3}:{2}:{4} {5}{6}'.format(begin, ts, __version__, __appname__, kind, msg, end)
        if sys.PY3:
            sys.stdout.write(s)
        else:
            sys.stdout.write(s.encode('utf8'))
        sys.stdout.flush()
    except:
        pass
        traceback.print_exc()

def handle_commandline():
    global __profile__, __index__
    if sys.PY3:
        from urllib.parse import unquote
    else:
        from urllib import unquote
    args = []
    kwargs = {}
    sys.stdin.close()
    _argv = sys.argv[1:]
    #if os.isatty(sys.stdin.fileno()):
    #    _argv = sys.argv[1:]
    #else:
    #    _argv = sys.stdin.read().split(' ') + sys.argv[1:]
    for x in _argv:
        if sys.PY3:
            pass
        else:
            x = x.decode('utf8')
        i = x.find('=')
        if i > -1:
            k, x  = x[:i], x[i+1:]
        else:
            k = None
        if k:
            v = unquote(x).split(',')
            if len(v) > 1:
                kwargs[unquote(k)] = tuple(_int(x) for x in v)
            else:
                kwargs[unquote(k)] = _int(v[0])
        else:
            if x:
                v = unquote(x).split(',')
                if len(v) > 1:
                    args.append(tuple(_int(x) for x in v))
                else:
                    args.append(_int(v[0]))
    if "profile" in kwargs:
        __profile__ = kwargs.pop("profile")
    if "index" in kwargs:
        __index__ = kwargs.pop("index")
    return args, kwargs

def _int(x):
    try:
        fx = float(x)
        ix = int(fx)
        return ix if ix == fx else fx
    except:
        return x


########################################################################


if "__main__" == __name__:
    main()
