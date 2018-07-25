#coding: utf-8

__appname__ = 'sse_server'
__version__ = '2017.233.1500' #fix nginx proxing
#__version__ = '2017.232.1200' #first edition

__profile__ = ""
__index__   =-1

import sys
import time
import queue
import random
import threading
import traceback
import subprocess


import libs.lib as lib


sys._SSE = {} #словарь текущих SSE подключений

def main():
    app_conf = init()
    try:
        app_conf['sse_server'].serve_forever()
    except KeyboardInterrupt as Err:
        print('Keyboard break, exiting', flush=True)
    except Exception as Err:
        print('Errr', flush=True)
    finally:
        lib.shutdown(app_conf)
    return

def init():
    app_conf = {}
    app_conf['extip'] = None
    app_conf['intip'] = None
    while not app_conf['extip']:
        app_conf['extip'], app_conf['intip'] = lib.getip()
    print('Server class: {0}'.format('SSE'), flush=True)
    print('Int IP: {0}, ext IP: {1}'.format(app_conf['intip'], app_conf['extip']), flush=True)
    rc = 0
    #starting rpc-sse server
    sse_server = lib.sseServer(('127.0.0.1', 0), lib.sseHandler)
    srv_host, srv_port = sse_server.socket.getsockname()
    app_conf['sse_server'] = sse_server
    data = """location /events {
    limit_except GET POST HEAD{
        deny all;
    }
    proxy_buffering off;
    chunked_transfer_encoding off;
    proxy_cache off;
    proxy_pass http://%s:%s; #%s
    }
location /sse {
    add_header Cache_Control no-cache;
    try_files $uri $uri/index.html $uri.html = 404;
}
""" % (srv_host, srv_port, __appname__)
    filelocation = f"/ms71/conf/location/{__appname__}"
    app_conf['filelocation'] = filelocation
    with open(filelocation, "wb") as f:
        f.write(data.encode())
    rc = subprocess.call(['sudo', 'nginx', '-s', 'reload', '-c', '/ms71/saas.conf', '-p', '/ms71/'])
    if 0 == rc:
        print('Serving SSE server at {0}:{1}'.format(srv_host, srv_port), flush = True)

        threading.Thread(target=lib.send_data, args=(), daemon=True).start() #send data via SSE

        return app_conf
    raise SystemExit(rc)




if "__main__" == __name__:
    main()
