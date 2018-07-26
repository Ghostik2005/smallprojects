#coding: utf-8

import sys
import time
import uuid
import json
import socket
import os.path

class UDPSocket(socket.socket):

    def __init__(self, bind_addr=('127.0.0.1', 0), std_addr=('127.0.0.1', 7122),
                 family=socket.AF_INET, s_type=socket.SOCK_DGRAM, proto=0, _sock=None):
        super(UDPSocket, self).__init__(family=family, type=s_type, proto=proto)
        try:
            self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except:
            pass
        try:
            self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except:
            pass
        self.bind(bind_addr)
        self._buf = []
        self._std_addr = std_addr

    def write(self, text):
        fg = False
        if isinstance(text, str):
            self._buf.append(text.encode())
            fg = text.rfind('\n') > -1
        else:
            self._buf.append(text)
            fg =  text.rfind(b'\n') > -1
        if fg:
            data = b''.join(self._buf)[:8192]
            self._buf.clear()
            return self.sendto(data, self._std_addr)

    def flush(self):
        pass


def udp_send(production=True, port=7122):
    if production:
        udpsock = UDPSocket(std_addr=('127.0.0.1', port))
    else:
        udpsock = sys.stdout
    pid = os.getpid() #pid of service
    uid = uuid.uuid4().hex #guid of service
    sys.argv[0] = os.path.abspath(sys.argv[0])#full path to running script.
    f_size = os.path.getsize(sys.argv[0]) #size of running file
    m_time = os.path.getmtime(sys.argv[0]) #last modify time of running file
    argv = '|'.join(m for m in sys.argv) #formated string from sys.argv
    while True: #infinite loop for heart beating
        try:
            payload = {"appname": sys.__appname__, "version": sys.__version__, "profile": sys.__profile__, "index": sys.__index__, "pid": pid, "uid": uid,
                   "extip": sys.extip, "intip": sys.intip, "argv": argv, "m_time": m_time, "size": f_size, "port": sys.s_port or 0}
        except:
            payload = {"appname": "not defined", "version": "not defined", "profile": "not defined", "index": "not defined", "pid": pid, "uid": uid,
                   "extip": "not defined", "intip": "not defined", "argv": argv, "m_time": m_time, "size": f_size, "port": "not defined"}
        payload = json.dumps(payload, ensure_ascii=False) #heart beat message
        print(payload, file=udpsock) #send to UDP socket our payload
        time.sleep(2.5)

