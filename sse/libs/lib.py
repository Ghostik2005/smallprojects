#coding: utf-8

import os
import sys
import time
import queue
import random
import traceback
import subprocess
import socketserver
from http.server import BaseHTTPRequestHandler

def getip():
    """
    get ip's function
    """
    
    import socket
    _urls = ('https://sklad71.org/consul/ip/', 'http://ip-address.ru/show','http://yandex.ru/internet',
        'http://ip-api.com/line/?fields=query', 'http://icanhazip.com', 'http://ipinfo.io/ip',
        'https://api.ipify.org')
    s = r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}"
    eip = None
    iip = ''
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as se:
            se.connect(("77.88.8.8", 80))
            iip = se.getsockname()[0]
    except Exception as e:
        print(f"err:{str(e)}", flush=True)
    import ssl, re, urllib.request
    ssl._create_default_https_context = ssl._create_unverified_context
    for url in _urls:
        r = None
        data = ''
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                data = str(r.headers)
                data += r.read().decode()
                eip = re.findall(s, data)[0].strip()
                break
        except Exception as e:
            continue
    return eip, iip

class sseServer (socketserver.ThreadingMixIn, socketserver.TCPServer):
    #класс JSONRPCServer
    daemon_threads = True
    allow_reuse_address = True
    _send_traceback_header = False
    
    def handle_error(self, request, client_address):
        pass
        return

class sseHandler(BaseHTTPRequestHandler):

    sse_path = '/events/SSE'

    def send_200(self, cl_id):
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Connection", "keep-alive")
            self.send_header("Pragma", "no-cache")
            self.end_headers()
        except Exception as Err:
            print(f'client: \'{cl_id}\' error ->> {traceback.format_exc()}')

    def send_err(self, cl_id):
        if cl_id:
            code = 423
            response = '[GET] Locked {0}: change the uri to {1}\n'.format(self.path, self.sse_path).encode()
        else:
            code = 401
            response = '[GET] Unauthorized: you have to provide your id\n'.encode()
        try:
            self.send_response(code)
            self.send_header("Content-type", "text/plain")
            self.send_header("Content-length", str(len(response)))
            self.send_header("Cache-control", "no-cache")
            self.end_headers()
            self.wfile.write(response)
            data = "retry: {0}\n\n".format('5')
            self.wfile.write(data.encode())
            self.wfile.flush()
        except Exception as Err:
            print(f'client: \'{cl_id}\' error ->> {traceback.format_exc()}')

    def do_GET(self):
        try:
            self.path, cl_id = self.path.split('?')
        except:
            cl_id = None
        if not self.path.endswith('/SSE') or not cl_id:
            self.send_err(cl_id)
        else:
            self.close_connection = 0
            self.send_200(cl_id)
            q = queue.Queue()
            # new connect
            sys._SSE[cl_id] = q
            d2 = 'welcome %s !' % (cl_id)
            data = "event: greating\ndata: %s\n\n" % d2
            try:
                self.wfile.write(data.encode())
                self.wfile.flush()
            except: pass
            while True:
                params = None
                #отсылаем или событие, если оно есть, или пустое сообщение
                try:
                    params = q.get(block=True, timeout=1+random.random())
                    data = 'event: %s\ndata: %s\nid: %s\n\n' % (params[0], params[1], params[2])
                    q.task_done()
                except queue.Empty:
                    time.sleep(random.random())
                    data = ":\n\n"
                try:
                    self.wfile.write(data.encode())
                    self.wfile.flush()
                except Exception as Err:
                    print('except while main writing', flush=True)
                    print(f'client: \'{cl_id}\' error ->> connection lost', flush=True)
                    #print(Err)
                    break
            # close connect
            try: sys._SSE.pop(cl_id)
            except: pass
            try: self.wfile.close()
            except: pass
            try: self.rfile.close()
            except: pass


def shutdown(app_conf):
    """
    function, runs when exiting
    """
    try:
        os.remove(app_conf["filelocation"])
    except: pass
    subprocess.call(['sudo', 'nginx', '-s', 'reload', '-c', '/ms71/saas.conf', '-p', '/ms71/'])
    try:
        app_conf['sse_server'].server_close()
    except Exception as Err:
        print(Err)

def send_data():
    #функция шлет данные во все открытые SSE соединения
    data = None
    c = 0
    for data in data_gen():
        if data:
            c += 1
            v = list(sys._SSE.values())
            params = ['message', data, c]
            while v:
                _q = v.pop()
                _q.put(params)
        time.sleep(4)

def data_gen():
    c = 0
    while True:
        c += 1
        payload = random.random()*120
        data = f'Iteration {c}, payload: {payload}'
        yield data



