# coding: utf-8

__appname__ = "sqlite2bz2"
__version__ = "17.242.1100" #исправлена методика округление цены при умножении на коэффициент
#__version__ = "17.242.1000" #исправлено приведение цены к минимуму и максимуму если они указаны
#__version__ = "17.241.1600" #первая редакция

import os
import bz2
import sys
import json
import time
import sqlite3
import traceback

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='UTF-8', buffering=1)
sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='UTF-8', buffering=1)
sys.name = None
sys.rpc = None

def main(filename, dest_folder):
    url = 'https://sklad71.org/apps/plxsrv/uri/RPC2'
    api_key = ''
    with open('api.api', 'rb') as fo:
        api_key = fo.read().decode().strip()
    sys.rpc = cli.ServerProxy(url, verbose=False, api_key=api_key)
    try:
        rr = dir(sys.rpc.plx.get_spr_id)
    except Exception as Err:
        print('Не возможно соединиться с сервисом plxsrv', flush=True)
        return

    con = cur = None
    try:
        p1 = os.path.basename(filename)
        f_n, _ = p1.split('.')
    except Exception as Err:
        print('файл должен быть с расширением', flush=True)
        return
    sys.name = f_n[-5:]
    con, cur = open_db(filename)
    cli_dict = get_clients(con, cur)
    ret_rows = []
    ret_rows.append(gen_f_row(cli_dict))
    print('инн с номерами колонок получены', flush=True)
    gen1 = pump_price(con, cur, cli_dict)
    cc = 0
    for d_row in gen1:
        cc += 1
        if cc % 100 == 0:
            print(f'\rобработанно товарных единиц -> {cc}', end='', flush=True)
        ret_rows.append(d_row)
    print(f'\rобработанно товарных единиц -> {cc}', flush=True)
    data = '\n,'.join(ret_rows)
    data = f'[{data}\n]'
    fn = sys.name + '.bz2'
    dest_f_name = os.path.join(dest_folder, fn) if dest_folder else fn
    with bz2.open(dest_f_name, 'wb') as f_obj:
        f_obj.write(data.encode())
    print(f'результат сохранен в -> {dest_f_name}', flush=True)
    cur.close()
    con.close()

def gen_f_row(cli_dict):
    ret = {}
    for n_row in cli_dict:
        for inn in cli_dict[n_row][1]:
            key = f'{inn}_{cli_dict[n_row][2]}' if cli_dict[n_row][2] != 1000000 else inn
            ret[key] = n_row
    return json.dumps(ret, ensure_ascii=False)

def pump_price(con, cur, cli_dict):
    sql_1 = f'select * from v{sys.name}r'
    cur.execute(sql_1)
    rows = cur.fetchall()
    for row in rows:
        d_row = []
        code, good, vendor, maxpack, rest, reestr, life, round_t, minzak, src_good, src_vendor, barcode, nds, id_spr, uid = row
        good_l = [good, '', src_good]
        vendor_l = [vendor, src_vendor]
        if not maxpack:
            maxpack = ''
        price_values = []
        for cli in cli_dict:
            nm_prc = cli_dict[cli][0]
            cf_prc = cli_dict[cli][2]
            dt_prc = cli_dict[cli][3]
            cur.execute(f"select * from v{sys.name}p where code = '{code}' and nm_prc = '{nm_prc}'")
            rows_p = cur.fetchall()
            try:
                rows_p = rows_p[0]
            except:
                rows_p = code, nm_prc, 0, 0, 0
            code, nm_prc, price, maxprice, minprice = rows_p
            price = int(round(price *100 * cf_prc / 1000000))
            if maxprice and price > maxprice:
                price = maxprice
            if minprice and price < minprice:
                price = minprice
            price_values.append(price)

        if not id_spr or -1 == idspr:
            id_spr = get_id_spr(src_good, src_vendor, code)
        d_row = [dt_prc, sys.name, price_values, code, good_l, vendor_l, maxpack, rest, reestr, life, round_t, minzak, nds, id_spr]
        yield json.dumps(d_row, ensure_ascii=False)

def get_id_spr(good, vendor, code):
    ret = sys.rpc.plx.get_spr_id(int(sys.name), good, vendor, code)
    rr = list(ret)
    for q in range(len(rr)):
        if not rr[q]:
            rr[q] = '--None'
        rr[q] = str(rr[q])
    if ret[-1] == -1:
        with open(f'/home/user/projects/saas/temps/dump_2sql_{sys.name}.txt', 'ab') as f_o:
            f_o.write('::'.join([good, vendor, code]).encode())
            f_o.write('\n||'.encode())
            f_o.write('::'.join(rr).encode())
            f_o.write('\n'.encode())
    return ret[-1]

def get_clients(con, cur):
    cli_dict = {}
    c = 0
    sql_1 = f"select distinct nm_prc, cf_prc from v{sys.name}c"
    cur.execute(sql_1)
    rows = cur.fetchall()
    for nm_prc, cf_prc in rows:
        inns_list = []
        cur.execute(f"select inn, dt_prc from v{sys.name}c where nm_prc = '{nm_prc}' and cf_prc = {cf_prc}")
        inns = cur.fetchall()
        for inn, dt_prc in inns:
            inns_list.append(inn)
        cli_dict[c] = (nm_prc, inns_list, int(cf_prc*1000000), dt_prc)
        c += 1
    return cli_dict


def open_db(filename):
    con=cur=None
    con = sqlite3.connect(filename, isolation_level='EXCLUSIVE')
    cur = con.cursor()
    try:
        cur.execute('PRAGMA synchronous = OFF;')
        cur.execute('PRAGMA journal_mode = OFF;')
    except Exception as Err:
        print(Err, flush=True)
    print(f'база данных -> {filename} <- подключена', flush=True)
    return con, cur

if __name__ == '__main__':
    t1 = time.time()
    try:
        filename = sys.argv[1]
    except Exception as Err:
        print('нужно указать имя файла', flush=True)
        sys.exit(0)
    if not os.path.exists(filename):
        print(f'{filename} отсутсвует', flush=True)
        sys.exit(0)
    try:
        dest_folder = sys.argv[2]
    except:
        print('папка для результата не задана', flush=True)
        print('используем текущую', flush=True)
        dest_folder = None
    try:
        main(filename, dest_folder)
    except Exception as Err:
        print(traceback.format_exc(), flush=True)
    t2 = time.time()
    tt = time.strftime("Общее время выполнения: %H:%M:%S", time.gmtime(t2 - t1))
    print(tt, flush=True)
    sys.exit()
