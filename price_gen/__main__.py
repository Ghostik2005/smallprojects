#coding: utf-8

__appname__ = 'price_generator'
__version__ = '2017.282.1454'

import os
import sys
import time
import atexit
import traceback
import configparser
from libs.lib_1 import main as main_logic
import libs.lib_1 as _lib

work_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

def main ():
    """
    1. проверяем, есть ли неотправленные файлы xlsx +
    2. отправляем файлы, их имена записываем, после отправки удаляем +
    3. считываем конфигурации +
    4. если в конфигурации есть инн только что отправленных файлов, то пропускаем обработку +
    5. формируем новые файлы с оставшимися инн +
    6. пишем файлы во временную папку +
    7. когда все сформированно, отправляем файлы +
    8. после отправки - удаляем файлы +
    """
    tmp_dir = os.path.join(work_dir, 'tmp')
    if not os.path.exists(tmp_dir):
        try:
            os.mkdir(tmp_dir, mode=0o777)
        except:
            print('не возможно создать рабочую папку', flush=True)
            return

    #считываем конгурацию нашего ящика
    config = configparser.ConfigParser()
    config.read(f'{work_dir}/mail.ini', encoding='UTF-8')
    mail = config['mail']
    m_params = {'host': mail['server'], 'port': int(mail['port']), 'ssl': bool(mail['ssl']), 'login': mail['login'], 'password': mail['password'], 'tmp': tmp_dir}

    #отправляем неотправленные, если есть
    ret_list = _lib.send_mail(m_params)

    #считываем данные для формирования файлов на отправку
    config.read(f'{work_dir}/conf.ini', encoding='UTF-8')
    params = {}
    for sn in config.sections():
        if not sn.startswith('customer'):
            continue
        params[sn] = {}
        for k in config[sn]:
            params[sn][k] = config[sn].get(k)
    while params:
        _, p = params.popitem()
        mails = p.pop('mail')
        inn = p.get('inn')
        if inn in ret_list:
            print(inn, '  -> пропускаем, только что отправили', flush=True)
            continue
        print(inn, '  -> обрабатываем', flush=True)
        mails = _lib.get_mails(mails)
        f_name = main_logic(p, tmp_dir)
        f_mails = f_name.replace('xlsx', 'mail')
        with open(f_mails, 'wb') as f_obj:
            f_obj.write(mails.encode())
    ret_list = _lib.send_mail(m_params)
    while ret_list:
        i = ret_list.pop(0)
        print(i, '  -> отправлено', flush=True)

def pid_alive(pid=None, pid_path=None):
    import sqlite3
    if not pid:
        pid = os.getpid()
    if not pid_path:
        pid_path = os.path.splitext(os.path.abspath(sys.argv[0]))[0] + '.pid'
    isLocked = False
    con=cur=None
    try:
        con = sqlite3.connect(pid_path, timeout=0.2, isolation_level='EXCLUSIVE')
        con.executescript("PRAGMA page_size = 1024; PRAGMA journal_mode = MEMORY; PRAGMA synchronous = OFF;")
        cur = con.cursor()
        sql = "CREATE TABLE PID (ID INTEGER NOT NULL, DT TIMESTAMP NOT NULL DEFAULT CURRENCY_TIMESTAMP);"
        cur.execute(sql)
        con.commit()
    except sqlite3.OperationalError as e:
        s = e.args[0].lower()
        if s.find('table pid already exists')>-1:
            pass
        elif s.find('database is locked')>-1:
            isLocked = True
        else:
            print(e.__class__, e, flush=True)
    except Exception as e:
        print(e.__class__, e, flush=True)
    finally:
        if isLocked:
            if con:
                con.close()
            con=cur=None
        else:
            cur.execute('INSERT INTO PID(ID)VALUES(?)', (pid,))
    sys.PID = [pid, pid_path, con, cur]
    return isLocked

def pid_close():
    pid, pid_path, con, cur = sys.PID
    sys.PID = []
    if cur:
        cur.close()
    if con:
        con.close()
    try: os.remove(pid_path)
    except: pass

if "__main__" == __name__:
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='UTF-8', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='UTF-8', buffering=1)
    _pid_path = '/tmp/' + os.path.splitext(os.path.basename(sys.argv[0]))[0] + '.lock'
    if pid_alive(pid_path=_pid_path):
        print('%s is worked' % 'price generator', flush=True)
        sys.exit(0)
    atexit.register(pid_close)
    t1 = time.time()
    try:
        main()
    except Exception as Err:
        print(Err, flush=True)
        print(traceback.format_exc(), flush=True)
    t2 = time.time()
    dt = t2 - t1
    print(time.strftime("Total executed time: %H:%M:%S", time.gmtime(dt)), flush=True)
        
