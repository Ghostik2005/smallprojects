#coding: utf-8

import sys
import time
import threading
import udpsend

if "__main__" == __name__:
    sys.intip = "192.168.0.12"
    sys.extip = "82.112.0.110"
    sys.s_port = None
    sys.__appname__ = "test1"
    sys.__version__ = "18.199.1000"
    sys.__profile__ = "profile_1"
    sys.__index__ = 1
    threading.Thread(target=udpsend.udp_send, args=(False,), daemon=True).start()
    while True:
        time.sleep(5)



    

