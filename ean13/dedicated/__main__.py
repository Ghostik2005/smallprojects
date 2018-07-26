# coding: utf-8

import os
import sys
import time
import socket
import traceback
import threading
import subprocess
from urllib.parse import unquote

import getip
import udpsend
from lockfile import LockWait
sys.PY3 = sys.version_info[0] > 2

__appname__ = "ean13"
__version__ = '18.207.0950' #добавлена библиотека определения ip сервиса, добавлена функция HeartBeat
#__version__ = '18.071.1320' #переделаны ценники на образцы, заменены шрифты и содержание
#__version__ = '2017.255.1800' #version for superwarden update
#__version__ = '2017.219.1530' #version for superwarden
#__version__ = '2017.188.1700' #рефакторим код, убираем все лишнее в формировании pdf и ods,
                              #количество этикеток в ODS печатается из запроса
#__version__ = '2017.187.1100'
#__version__ = '2017.184.1800'
__profile__ = ""
__index__   =-1
__hostname__ = socket.gethostname().lower()

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
    ips = getip.ip()
    sys.intip = ips.get('int_ip')
    sys.extip = ips.get('ext_ip')
    t1 = threading.Timer(10, threading.Thread(target=udpsend.udp_send, args=(), daemon=True).start)
    t1.start()
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
    import zlib
    import urllib.parse as urllib2
    from io import BytesIO as StringIO
    from ean13.ean2pdfV2 import main2
    from ean13.ean2odsV2 import toods

    def saveTxt2Pdf(cp, text, method=None):
        try:
            output = StringIO()
            rows = text.splitlines()
            main2(__appname__, rows, output, method)
            data = output.getvalue()
            return data
        except:
            log(traceback.format_exc(), kind="error")
        finally:
            output.close()

    def saveTxt2Ods(cp, text, method=None):
        try:
            output = StringIO()
            rows = text.splitlines()
            toods(__appname__, rows, output, method)
            data = output.getvalue()
            return data
        except:
            log(traceback.format_exc(), kind="error")
        finally:
            output.close()

    addr, pid = env["scgi.initv"][:2]
    msg = f'{addr[0]} {addr[1]} {env["HTTP_METHOD"]} {env["URI"]} {env["HTTP_PARAMS"]} {env["HTTP_KWARGS"]}'
    env["scgi.defer"] = lambda: log("%s close" % msg)
    log(msg)
    uri = env["URI"].lower()
    qw = uri.split('/ean13')
    method = qw[-1]
    cp = 'utf8'
    q = None
    _rm = env["HTTP_METHOD"].upper()

    #env["HTTP_PARAMS"] = u'1251&Товар1;1007290004649;1;123.45&Товар2;1007290004977;2;678,90'
    #env["HTTP_PARAMS"] = 'ods&Товар1;1007290004649;1;123.45&Товар2;1007290004977;2;678,90'
    #env["HTTP_PARAMS"] = 'Товар1;1007290004649;1;123.45&Товар2;1007290004977;2;678,90'
    #env["HTTP_PARAMS"] = 'Аптека низких цен!~Мезим фортеТовар мазь крем для рук Мезим форте;1007290004649;1;123.45&Товар2;1007290004977;2;12223.45'

    try:
        _qs = '&'.join(env["HTTP_PARAMS"])
    except:
        _qs = ''
    fgOds = _qs.find('ods')>-1
    LastModified = time.strftime('%a, %d %b %Y %X GMT', time.gmtime())
    content = ''
    header = []

    #print(_rm)
    if 'HEAD' == _rm:
        header = head(LastModified, len(content), True)
    elif 'POST' == _rm:
        q = env['scgi.rfile'].read(env['CONTENT_LENGTH'])
        try:
            q = zlib.decompress(q)
        except:
            pass
        if q:
            try:
                _cp = _qs[:4]
                if '1251' == _cp:
                    cp = _cp
                q = q.decode(cp)
            except:
                pass

            fgOds_p = q.find('ods')>-1
            log(q, kind='REQUEST')
            if fgOds or fgOds_p:
                content = saveTxt2Ods(cp, q, method)
                header = head(LastModified, len(content), False, fgOds)
                #with open('11.ods', 'wb') as ff:
                    #ff.write(content)
            else:
                ww = saveTxt2Pdf(cp, q, method)
                content = zlib.compress(ww)
                header = head(LastModified, len(content), True, fgOds)
    elif 'GET' == _rm:
        try:
            _cp = _qs[:4]
            if '1251' == _cp:
                cp = _cp
                q = urllib2.unquote(_qs[4:].replace(';', '\t').replace('&', '\n'))
            else:
                q = urllib2.unquote(_qs.replace(';', '\t').replace('&', '\n'))
            try:
                q = q.decode(cp)
            except:
                pass
        except:
            q = ''
        if q:
            log(q, kind='REQUEST')
            if fgOds:
                content = saveTxt2Ods(cp, q, method)
                header = head(LastModified, len(content), False, fgOds)
            else:
                ww = saveTxt2Pdf(cp, q, method)
                content = zlib.compress(ww)
                header = head(LastModified, len(content), True, fgOds)
        else:
            header = [("Last-Modified", "%s" % LastModified),
                    ("Cache-Control", "no-cache"),
                    ("Content-Type", "text/plain; charset=UTF-8"),
                    ("X-Accel-Buffering", "no")
                    ]
            content = u"""Манускрипт.Онлайн

Сервис EAN13 формирует pdf или ods файл пригодный для печати этикеток штрих-кодов формата 30x20.

Пример использования (метод GET):
для обычных ценников:
  https://online365.pro/ean13?1251&ods&Товар1;1007290004649;1;123.45&Товар2;1007290004977;2;678,90
для увеличенного шрифта:
  https://online365.pro/ean13big?1251&ods&Товар1;1007290004649;1;123.45&Товар2;1007290004977;2;678,90
для увиличенного жирного шрифта:
  https://online365.pro/ean13bigbold?1251&ods&Товар1;1007290004649;1;123.45&Товар2;1007290004977;2;678,90
где
  1251          - не обязательный параметр указывающий в какой кодировке передаются данные,
                  если отсутствует, то данные должны быть в utf8
  ods           - не обязательный параметр указывающий в каком формате возвращать данные,
                  если отсутствует, то данные вернутся в pdf
  Товар1        - имя товара до 105 символов
                  товар можно передать с доп. информацией, например,
                  Текст1~Товар1
                  2|Текст1~Товар1
                  где 2| количество повторений доп. информации
  1007290004649 - штрих-код товара
  1             - кол-во повторений штрих-кода для данного товара
  123.45        - цена товара, в цене допускается разделитель "." или ","
                  цену можно передать с доп. информацией, например,
                  Текст1~123.45~txt2~txt3
                  00031~123.45~29.06~15

Пример использования (метод POST):
  https://online365.pro/ean13 - для обычных ценников
  https://online365.pro/ean13big - для увеличенного шрифта
  https://online365.pro/ean13bigbold - для увиличенного жирного шрифта

  данные передаются в "текстовом формате резделитель табуляция", метод сжатия - deflate
  Товар1 <tab> 1007290004649 <tab> 1 <tab> 123.45 <newline>
  Товар2 <tab> 1007290004977 <tab> 2 <tab> 678,90 <newline>
""".encode('utf-8')

    # три обязательных вызова yield: статус, заголовки, содержание
    yield '200 OK'
    yield header
    yield content


def head(aLastModified, aContentLength, fgDeflate=True, fgOds=False):
    r = []
    r.append(("Last-Modified", "%s" % aLastModified))
    r.append(("Content-Length", "%i" % aContentLength))
    if fgOds:
        r.append(("Content-Disposition", "attachment; filename=ean13.ods"))
        r.append(("Content-Type", "application/octet-stream"))
    else:
        r.append(("Content-Disposition", "attachment; filename=ean13.pdf"))
        r.append(("Content-Type", "application/pdf"))
    if fgDeflate:
        r.append(("Content-Encoding", "deflate"))
    return r

def init(sock):
    addr = sock.getsockname()[:2]
    sock.listen(25)
    APPCONF["addr"] = addr
    sys.s_port = addr[1]
    sys.__appname__ = __appname__
    sys.__version__ = __version__
    sys.__profile__ = __profile__
    sys.__index__ = __index__
    fileupstream = _getfilename("upstream")
    APPCONF["fileupstream"] = fileupstream
    data = """location /ean13 {
    limit_except HEAD GET POST {
        deny all;
    }
    include scgi_params;
    scgi_read_timeout          120s;
    scgi_pass ean13d;
    sendfile                  on;
    scgi_buffering            off;
    scgi_cache                off;
    gzip                      on;
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
    data1 = """upstream ean13d {
    least_conn;
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
                #print('>>>', i, src[i], _find, src[i].find(_find))
            if fg:
                src[len(src)-1] = ' ' + data2[1:] + '\n}\n'
            src = '\n'.join(src)
            with open(_fileupstream, "wb") as f:
                f.write(src.encode())
        else:
            with open(_fileupstream, "wb") as f:
                f.write(data1.encode())
    rc = 0
    #rc = subprocess.call(['sudo', 'nginx', '-t', '-c', '/ms71/saas.conf', '-p', '/ms71/'])
    rc = subprocess.call(['nginx', '-t'])
    if 0 == rc:
        #rc = subprocess.call(['sudo', 'nginx', '-s', 'reload', '-c', '/ms71/saas.conf', '-p', '/ms71/'])
        rc = subprocess.call(['nginx', '-s', 'reload'])
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
            fg_noapp = 0 == len(src[2:-1])
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
    #subprocess.call(['sudo', 'nginx', '-s', 'reload', '-c', '/ms71/saas.conf', '-p', '/ms71/'])
    subprocess.call(['nginx', '-s', 'reload'])
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
        ###
        rfile = conn.makefile("rb", -1)
        wfile = conn.makefile("wb", 0)

        env = _env_read(rfile)
        env = _args_parse(env)
        #env["scgi.connection"] = conn
        #env["scgi.address"] = addr
        env["scgi.defer"] = None
        env["scgi.initv"] = initial_value
        env["scgi.rfile"] = rfile
        #env["scgi.rfile"] = conn
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
        #print("BrokenPipeError", file=sys.stderr, flush=True)
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
        #print("ENV")
        #print(env)
        if env and env.get("scgi.defer"):
            try:
                env["scgi.defer"]()
                #print("DEFER:DONE")
            except:
                #print("DEFER:ERROR")
                #traceback.print_exc()
                log(traceback.format_exc(), kind="error:defer")
        #print("active_count:", threading.active_count() - 2, flush=True)

# netstring utility functions
def _env_read(f):
    #print('f : ', f)
    fr = f.read(16)
    #print('fr: ', fr)
    size, d = fr.split(b':', 1)
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
    #print('-'*16)
    while items:
        v = items.pop()
        k = items.pop()
        #print("env k:", type(k), len(k), k)
        #print("env v:", type(v), len(k), v)
        #print()
        env[k.decode()] = v.decode()
    #print('+'*16)
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
    if sys.stdout.encoding != 'UTF-8':
        sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1, encoding='UTF-8')
    if sys.stderr.encoding != 'UTF-8':
        sys.stderr = open(sys.stderr.fileno(), mode='w', buffering=1, encoding='UTF-8')
    main()
