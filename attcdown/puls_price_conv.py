# coding: utf-8

__appname__ = "puls_price_converter"
__version__ = "17.233.1000" #pid locking update, fix coding troubles
#__version__ = "17.229.1140" #update pathes, working with pid-file, code readability
#__version__ = "17.228.1800" #first edition

import os
import sys
import glob
import time
import email
import poplib
import traceback
from email.header import decode_header

work_dir = os.path.dirname(os.path.abspath(sys.argv[0])) #work folder

def main():
    app_conf = _pre()
    if not app_conf:
        print('!!! Error -> no password file', flush=True)
        return True 
    price_get(app_conf)
    converter(app_conf)
    return True

def _pre():
    app_conf = {}
    pass_path = os.path.join(work_dir, 'online365.pass') #file with password
    if os.path.exists(pass_path):
        with open(pass_path, 'r') as f:
            data = f.read().strip().split('\n')
        app_conf['user'] = data[0].strip()
        app_conf['password'] = data[1].strip()
    else:
        return False
    #28176 = puls76
    app_conf['s_path'] = '/tmp/28176_tmp' #tepmorary folder
    app_conf['d_path'] = '/home/plexpert/www/puls76/prices' #destination folder
    app_conf['out_file'] = 'price28176-2901161825.txt' #output txt-file name
    app_conf['mail_host'] = 'pop.yandex.ru' #server
    app_conf['mail_port'] = 995 #port
    app_conf['params'] = [
        'пульс',
        #'катрен'
        ]
    if not os.path.exists(app_conf['s_path']):
        os.makedirs(app_conf['s_path'], mode=0o777)
    if not os.path.exists(app_conf['d_path']):
        os.makedirs(app_conf['d_path'], mode=0o777)
    return app_conf

def puls_parse(filename):
    data = None
    with open(filename, 'rb') as f_obj:
        data = f_obj.read().decode('cp1251').replace(';', '\t')

#    self._pHead = {u'ЦЕНА': u'PRICE1', u'КОД': u'CODEPST', u'ТОВАР': u'NAME', u'ЗАВОД': [u'FIRM', u'CNTR'], # [завод, страна]
#            u'УПАК': u'QNTPACK', u'ОСТАТОК': u'QNT', u'РЕЕСТР': u'GNVLS',
#            u'ГОДЕН': u'GDATE', u'КРАТЗАК': u'MULTIPLC', u'МИНЗАК': u'MINORD', u'ШТРИХ': u'EAN13', u'НДС': u'NDS'}
#YYYYMMDD - mask for expire date

    header = 'CODEPST\tF2\tNAME\tF4\tPRICE1\tFIRM\tQNT\tQNTPACK\tF9\tF10\tF11\tGNVLS\tF13\tF14\tEAN13\tMULTIPLC\tGDATE\tCNTR'
    ret = []
    ret = _format_expire_date(data, -1)
    ret.insert(0, header)
    return '\n'.join(ret)

def _format_expire_date(data, date_position=-1):
    data = data.strip().split('\n')
    ret = []
    for lin in data:
        lin_data = lin.strip().split('\t')
        date, _ = lin_data[-1].split()
        date = date.strip().split('.')
        date.reverse()
        date = ''.join(date)
        lin_data[-1] = date
        lin_data.append(' ')
        ret.append('\t'.join(lin_data))
    return ret

def converter(app_conf):
    for filename in get_names(app_conf["s_path"]):
        filename = os.path.abspath(filename)
        data = puls_parse(filename)
        if data:
            os.remove(filename)
            try: os.rmdir(os.path.dirname(filename))
            except: pass
            out_f_name = os.path.join(app_conf["d_path"], app_conf['out_file'])
            with open(out_f_name, 'wb') as out_f_obj:
                out_f_obj.write(data.encode())
            stri = f"-||converting: {os.path.basename(filename)} ->> {out_f_name}  - succesfully"
            print(stri, flush=True)
            time.sleep(1.1)

class Mail_POP3_SSL(poplib.POP3_SSL):

    def __enter__(self):
        return self

    def __exit__(self, *args):
        try:
            self.quit()
        except OSError:
            pass

def get_names(path='.'):
    ddd = glob.glob(path+'/*')
    for f_name in ddd:
        yield f_name

def msg_str_parse(v, decoder):
    return ''.join(w.decode(cp or 'utf8') if isinstance(w, bytes) else w for w, cp in decoder(v)) if v else ''

def price_get(app_conf):
    parse_params = app_conf['params']
    user = app_conf["user"]
    password = app_conf["password"]
    mail_host = app_conf["mail_host"]
    mail_port = app_conf["mail_port"]
    s_path = app_conf["s_path"]
    attach = False
    with Mail_POP3_SSL(host=mail_host, port=mail_port) as mail_box:
        uid_list = []
        mail_box.user(user)
        mail_box.pass_(password)
        print(f'-|connecting to mailbox {user}', flush=True)
        uid_list = mail_box.uidl()[1]
        while uid_list:
            uid = uid_list.pop().decode().split()[0]
            msg = b'\r\n'.join(mail_box.retr(uid)[1])
            message = email.message_from_bytes(msg)
            v = message['Subject']
            s = msg_str_parse(v, decode_header)
            for param in parse_params:
                if param in s.lower():
                    for part in message.walk():
                        if part.get_content_maintype() == 'multipart':
                            continue
                        if not part.get('Content-Disposition'):
                            continue
                        print(f'-|mail id:  {uid}, attachment found', flush=True)
                        filename = part.get_filename()
                        try: filename = decode_header(filename)[0][0].decode()
                        except: pass
                        if not filename:
                            filename = "unknown_name.txt"
                        f_path = os.path.join(s_path, filename)
                        with open(f_path, 'wb') as f:
                            f.write(part.get_payload(decode=1))
                            print(f'-|attachment -> {os.path.basename(f_path)} <- downloaded', flush=True)
                            mail_box.dele(uid) #deleting message, in production
                            attach = True
        if attach:
            print('-|all available prices downloaded', flush=True)

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

if __name__ == '__main__':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='UTF-8', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='UTF-8', buffering=1)
    _pid_path = '/tmp/' + os.path.splitext(os.path.basename(sys.argv[0]))[0] + '.lock'
    if pid_alive(pid_path=_pid_path):
        print('%s is worked' % 'puls price converter', flush=True)
        sys.exit(0)
    import atexit
    atexit.register(pid_close)
    
    try:
        main()
    except Exception as Err:
        print(traceback.format_exc(), flush=True)
    finally:
        sys.exit(0)
