#coding: utf-8
from __future__ import print_function
from pykeyboard import PyKeyboard
import sys
import time
import serial
try:
    from urllib.parse import unquote
except ImportError:
    from urllib import unquote
import traceback



def main():
    def s_open(s_s):
        old_msg = ''
        try:
            s_s.close()
        except:
            pass
        while True:
            try:
                s_s.open()
            except Exception:
                msg = traceback.format_exc()
                if msg != old_msg:
                    print(msg)
                    sys.stdout.flush()
                    old_msg = msg
                time.sleep(2)
            else:
                serial_conn.flushOutput()
                serial_conn.flushInput()
                break
        print(s_s)
        sys.stdout.flush()
        return s_s
        
    try:
        k = PyKeyboard()
    except:
        traceback.print_exc()
        return
    port, rate = parse_c_line()
    try:
        serial_conn = serial.Serial()
    except serial.SerialException:
        traceback.print_exc()
        return
    serial_conn.port = port
    if rate:
        serial_conn.baudrate = rate
    serial_conn.timeout = 0.2
    serial_conn = s_open(serial_conn)
    while True:
        try:
            #s = sys.stdin.readline().strip()
            s = serial_conn.readline().strip()
            if not s:
                continue
            k.type_string(s.decode('utf8'))
            k.tap_key(k.enter_key)
        except serial.SerialException:
            serial_conn = s_open(serial_conn)
        except KeyboardInterrupt:
            break
        except Exception as Err:
            print('*'*20)
            sys.stdout.flush()
            traceback.print_exc()
            #serial_conn.flushOutput()
            #serial_conn.flushInput()
    try:
        serial_conn.close()
    except:
        pass

def _int(x):
    try:
        fx = float(x)
        ix = int(fx)
        return ix if ix == fx else fx
    except:
        return x

def parse_c_line():
    args = []
    kwargs = {}
    sys.stdin.close()
    _argv = sys.argv[1:]
    for x in _argv:
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
                    args.append(tuple(libs._int(x) for x in v))
                else:
                    args.append(_int(v[0]))
    if "port" in kwargs:
        port = kwargs.pop("port")
    else:
        port = "/dev/ttyUSB0"
    if "rate" in kwargs:
        rate = kwargs.pop("rate")
    else:
        rate = None
    return port, rate

if "__main__" == __name__:
    main()
