# coding: utf-8

__appname__ = "price_converter_with_downloader"
__version__ = "17.226.1800" 

import os
import sys
import time
import email
import queue
import poplib
import getpass
import threading
import subprocess
from email.header import decode_header

class Mail_POP3_SSL(poplib.POP3_SSL):
    """
    class poplib.POP3_SSL with 'with' statement
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        try:
            self.quit()
        except OSError:
            pass

def msg_str_parse(v, decoder):
    return ''.join(w.decode(cp or 'utf8') if isinstance(w, bytes) else w for w, cp in decoder(v)) if v else ''

def price_get(app_conf):
    addrs = {
        #'price20229-2901161825@online365.pro': 'протек', #recieves directly from supplier
        #'price20129-2901161825@online365.pro': 'сиа', #recieves directly from supplier
        #'price28176-2901161825@online365.pro': 'пульс',
        #'price20576-2901161825@online365.pro': 'катрен',
        #'price51070-2901161825@online365.pro': 'мк',
        #'katren': 'katren',
        #'mkcompany': 'мк',
        #'puls': 'puls',
        #'пульс': 'пульс',
        'катрен': 'катрен'
        }
    user = app_conf.get("user")
    password = app_conf.get("password")
    mail_host = app_conf.get("mail_host")
    mail_port = app_conf.get("mail_port")
    s_path = app_conf.get("s_path")
    #parse_list = ['To', 'From', 'Subject']
    while True:
        attach = False
        with Mail_POP3_SSL(host=mail_host, port=mail_port) as mail_box:
            uid_list = []
            mail_box.user(user)
            mail_box.pass_(password)
            print('connecting to mailbox', flush=True)
            cnt_msg = mail_box.stat()[0]
            uid_list = mail_box.uidl()[1]
            #print(cnt_msg)
            while uid_list:
                uid = uid_list.pop(0).decode().split()[0]
                msg = b'\r\n'.join(mail_box.retr(uid)[1])
                message = email.message_from_bytes(msg)
                v = message['Subject']
                s = msg_str_parse(v, decode_header)
                for addr in addrs:
                    if addr in s.lower():
                        print(f'matched:  {s}', flush=True)
                        print(f'mail id:  {uid}', flush=True)
                #for stri in parse_list:
                    #v = message[stri]
                    #s = msg_str_parse(v, decode_header)
                    #multi = s.split(',')
                    #if 1 < len(multi):
                        #for x in multi:
                            #print(f"{stri}:  {email.utils.parseaddr(x)}", flush=True)
                    #else:
                        #print(f"{stri}:  {s}", flush=True)
                        for part in message.walk():
                            if part.get_content_maintype() == 'multipart':
                                continue
                            if not part.get('Content-Disposition'):
                                continue
                            filename = part.get_filename()
                            try:
                                filename = decode_header(filename)[0][0].decode()
                            except:
                                pass
                            if not filename:
                                filename = "unknown_name.txt"
                            attach = True
                            f_path = os.path.join(s_path, filename)
                            if os.path.exists(f_path):
                                f_pathes = os.path.splitext(f_path)
                                f_path = ''.join([f_pathes[0], '_', str(int(time.time())), f_pathes[1]])
                            with open(f_path, 'wb') as f:
                                f.write(part.get_payload(decode=1))
                                print(f'attachment -> {f_path} <- downloaded')
                                mail_box.dele(uid) #deleting message, in production
                                attach = True
        if attach:
            input('make a reserve copy of source')
            app_conf.get("f_queue").put(True)
        print('prices downloaded', flush=True)
        break
        time.sleep(300)

def converter_start(app_conf):
    base_path = os.path.dirname(sys.argv[0])
    conv_path = os.path.join(base_path, 'price_converter.zip')
    c_line = f'sudo {app_conf.get("miniconda")} {conv_path} -o {app_conf.get("d_path")} -s {app_conf.get("s_path")} -t {os.path.join(app_conf.get("d_path"), "temp")}'
    c_line = c_line.split()
    while True:
        try:
            attach = app_conf.get("f_queue").get()
        except Exception as Err:
            time.sleep(1)
        else:
            app_conf.get("f_queue").task_done()
            if attach:
                ww = subprocess.Popen(c_line).wait()
        break

def main():
    app_conf = {}
    path = '/ms71/data/price_converter'
    path1 = '/ms71/saas/prices'
    fn = os.path.join(path, 'pc.pid')
    s_path = os.path.join(path, 'source')
    d_path = os.path.join(path, 'destination')
    #path to miniconda python3.6
    miniconda = '/home/conda/miniconda3/bin/python3.6'
    miniconda = '/home/user/miniconda3/bin/python3.6'
    if not os.path.exists(s_path):
        os.makedirs(path, mode=0o777)
    if not os.path.exists(d_path):
        os.makedirs(d_path, mode=0o777)
    if os.path.exists(fn):
        return None
    with open(fn, 'w') as f_obj:
        f_obj.write('opened')
    mail_host = 'pop.yandex.ru'
    mail_port = 995
    f_queue = queue.Queue()
    pass_path = os.path.join(path1, 'online365.pass')
    if os.path.exists(pass_path):
        with open(pass_path, 'r') as f:
            data = f.read().strip().split('\n')
        user = data[0]
        password = data[1]
    else:
        user = input('username: ')
        password = getpass.getpass()
    #print(user, password)
    app_conf['s_path'] = s_path
    app_conf['d_path'] = d_path
    app_conf['mail_host'] = mail_host
    app_conf['mail_port'] = mail_port
    app_conf['user'] = user
    app_conf['password'] = password
    app_conf['miniconda'] = miniconda
    app_conf['f_queue'] = f_queue
    threads = []
    
    price_get(app_conf)
    converter_start(app_conf)
    #threads.append(threading.Thread(target=price_get, args=(app_conf, ), daemon=True))
    #threads.append(threading.Thread(target=converter_start, args=(app_conf, ), daemon=True))

    for thr in threads:
        thr.start()
    return fn

if __name__ == '__main__':
    try:
        fn = main()
    except Exception as Err:
        pass
    finally:
        if fn:
            try:
                os.remove(fn)
            except Exception as Err:
                pass
        else:
            print('another instance running', flush=True)
        sys.exit(1)

