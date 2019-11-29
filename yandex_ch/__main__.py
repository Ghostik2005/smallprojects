# coding: utf-8

from __future__ import absolute_import, with_statement, print_function, unicode_literals

__appname__ = '<appname>'
__version__ = '<version>'
__profile__ = 'default'
__index__   = 0

import sys, os
PY2 = sys.version_info[0] < 3
PY3 = sys.version_info[0] > 2
if __name__ == '__main__':
    # env PYTHONIOENCODING="UTF-8"
    #sys.path.insert(0, '/ms71/repo/msclib.zip')
    if PY2:
        reload(sys); sys.setdefaultencoding('UTF-8')
    else:
        if sys.stdout.encoding != 'UTF-8':
            sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1, encoding='UTF-8')
        if sys.stderr.encoding != 'UTF-8':
            sys.stderr = open(sys.stderr.fileno(), mode='w', buffering=1, encoding='UTF-8')
    s = os.path.dirname(__file__)
    sys.fg_zip = os.path.isfile(s) and s[-4:].lower() == '.zip'
if PY2:
    input = raw_input

import traceback, socket, time

if __appname__ == '<appname>':
    __appname__ = os.path.basename(__file__).split('.', 1)[0]
    if '__main__' == __appname__:
        i = __file__.rfind('__main__')
        if i > -1:
            s = __file__[:i]
            __appname__ = s[:-1].split(s[-1])[-1].split('.', 1)[0]
if __version__ == '<version>':
    __version__ = time.strftime("%y.%j.%H%M")
if not __index__:
    __index__ = os.getpid()
__hostname__ = socket.gethostname().lower()

def main():
    from libs.worker import start
    # from libs.worker import get_argvs
    try:
        sys.log('begin')
        start()
        # _start()
    except (KeyboardInterrupt, SystemExit) as e:
        pass
    except Exception as e:
        sys.log(e)
    finally:
        sys.log('end', kind='info', begin='\r')

_ts = "%Y-%m-%d %H:%M:%S"
def log(msg, kind='info', begin='', end='\n'):
    global _ts, __hostname__, __appname__, __profile__, __version__, __index__
    try:
        try: ts = time.strftime(_ts)
        except: ts = time.strftime(_ts)
        if msg is None:
            data = ''.join(
                ('%s %s %s.%s %s %s:%s %s\n' % (ts, __hostname__, __appname__,__profile__,__version__,__index__,'traceback', msg)
                if i else '%s %s %s.%s %s %s:%s\n' % (ts, __hostname__, __appname__,__profile__,__version__,__index__,msg)
                ) for i, msg in enumerate(traceback.format_exc().splitlines())
            )
        else:
            data = '%s%s %s %s.%s %s %s:%s %s%s' % (begin,ts, __hostname__, __appname__,__profile__,__version__,__index__,kind, msg,end)
        sys.stdout.write(data)
        sys.stdout.flush()
    except:
        pass

sys.log = log


if __name__ == '__main__':
    main()
