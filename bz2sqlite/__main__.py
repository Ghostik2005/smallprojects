# coding: utf-8

__appname__ = "bz2sqlite"
__version__ = "17.245.1100" #поиск id_spr если он не указан или -1
#__version__ = "17.242.1800" #исправлена цена реестра и оптимизирована втавка занчений
#__version__ = "17.240.1600" #первая редакция

import os
import bz2
import sys
import json
import time
import sqlite3
import traceback
import ms71lib.client as cli


sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='UTF-8', buffering=1)
sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='UTF-8', buffering=1)
sys.name = None
sys.rpc = None

def main(filename, dest_folder):
    url = 'https://sklad71.org/apps/plxsrv/uri/RPC2'
    api_key = ''
    with open('/home/user/git/api_k/api.api', 'rb') as f_o:
        api_key = f_o.read().decode().strip()
    sys.rpc = cli.ServerProxy(url, verbose=False, api_key=api_key)
    try:
        rr = dir(sys.rpc.plx.get_spr_id)
    except Exception as Err:
        print('Не возможно соединиться с сервисом plxsrv', flush=True)
        return
    con = cur = None
    try:
        p1 = os.path.basename(filename)
        sys.name, _ = p1.split('.')
    except Exception as Err:
        print('файл должен быть с расширением', flush=True)
        return
    #r = sys.rpc.plx.get_spr_id(30178, u"Либридерм Аевит набор подарочный(помада4г,крем30мл,маска д/л", u"Дина+ ООО - РОССИЯ", u"56654")
    #req = 20171, "Малавит 30мл фл. Б М .@", "Малавит ООО/ Ауровит ООО - РОССИЯ", '88406'
    #r = sys.rpc.plx.get_spr_id(*req)
    #print(req)
    #print(17666, r)
    #return

    con, cur = create_db(dest_folder)
    t_s = time.time()
    db_fill(filename, con, cur)
    print('заполнение базы завершено', flush=True)
    t_e = time.time()
    print(time.strftime("время заполнения: %H:%M:%S", time.gmtime(t_e - t_s)), flush=True)
    ret = create_idx(con, cur)
    if ret:
        print('индексы созданы', flush=True)
        print(time.strftime("время создания индексов: %H:%M:%S", time.gmtime(time.time() - t_e)), flush=True)
    cur.close()
    con.close()


def create_idx(con, cur):
    sql_req = [f"""CREATE UNIQUE INDEX V{sys.name}A_IDX1 ON V{sys.name}A (CD_ORG, CD_ADR)""",
f"""CREATE UNIQUE INDEX V{sys.name}C_IDX1 ON V{sys.name}C (CD_ORG)""",
f"""CREATE INDEX V{sys.name}C_IDX2 ON V{sys.name}C (INN)""",
f"""CREATE INDEX V{sys.name}C_IDX3 ON V{sys.name}C (NM_PRC)""",
f"""CREATE UNIQUE INDEX V{sys.name}P_IDX1 ON V{sys.name}P (CODE, NM_PRC)""",
f"""CREATE INDEX V{sys.name}P_IDX2 ON V{sys.name}P (NM_PRC)""",
f"""CREATE UNIQUE INDEX V{sys.name}R_IDX1 ON V{sys.name}R (CODE)"""]
    ret = True
    try:
        for sql in sql_req:
            cur.execute(sql)
        con.commit()
    except Exception as Err:
        ret = False
        print('ошибка при создании индексов', flush=True)
        print(Err, flush=True)
    return ret

def create_db(dest_folder):
    sql_req = [f"""CREATE TABLE V{sys.name}A (
  CD_ORG TEXT NOT NULL,
  CD_ADR TEXT NOT NULL,
  NM_ADR TEXT DEFAULT ''
  );""",
f"""CREATE TABLE V{sys.name}C (
  INN TEXT NOT NULL,
  CD_ORG TEXT NOT NULL,
  NM_ORG TEXT DEFAULT '',
  NM_PRC TEXT DEFAULT '',
  CF_PRC REAL DEFAULT 1,
  DT_PRC TIMESTAMP
  );""",
f"""CREATE TABLE V{sys.name}P (
  CODE TEXT,
  NM_PRC TEXT,
  PRICE REAL,
  MAXPRICE REAL DEFAULT 0.0,
  MINPRICE REAL DEFAULT 0.0
  );""",
f"""CREATE TABLE V{sys.name}R (
  CODE TEXT NOT NULL,
  GOOD TEXT,
  VENDOR TEXT,
  MAXPACK TEXT,
  REST INTEGER,
  REESTR REAL DEFAULT 0,
  LIFE DATE,
  ROUND INTEGER DEFAULT 0,
  MINZAK INTEGER DEFAULT 0,
  SRC_GOOD TEXT,
  SRC_VENDOR TEXT,
  BARCODE TEXT,
  NDS INTEGER DEFAULT 0,
  ID_SPR INTEGER DEFAULT -1,
  UID TEXT DEFAULT ""
  );"""]
    con=cur=None
    base_name = 'price' + sys.name + '.db3'
    if dest_folder:
        if not os.path.exists(dest_folder):
            try:
                os.makedirs(dest_folder)
            except Exception as Err:
                print('не возможно создать папку для результата, используем текущую', flush=True)
            else:
                base_name = os.path.join(dest_folder, base_name)
        else:
            base_name = os.path.join(dest_folder, base_name)
    if os.path.exists(base_name):
        os.remove(base_name)
    con = sqlite3.connect(base_name, isolation_level='EXCLUSIVE')
    cur = con.cursor()
    try:
        cur.execute('PRAGMA synchronous = OFF;')
        cur.execute('PRAGMA journal_mode = OFF;')
        cur.execute('PRAGMA page_size = 8192;')
        #cur.execute('PRAGMA cache_size = -1024;')
    except Exception as Err:
        print(Err, flush=True)
    try:
        for sql in sql_req:
            cur.execute(sql)
        con.commit()
    except Exception as Err:
        print(Err, flush=True)
    print('база данных создана', flush=True)
    return con, cur


def db_fill(filename, con, cur):
    renames = []
    drops = []
    res = ''
    inns_spr = []
    gen1 = main_pump(filename)
    prDate, inns = gen1.__next__()
    sql_0 = "insert into v%sc values (?, ?, ?, ?, ?, ?);" % sys.name
    sql_1 = "insert into v%sp values (?, ?, ?, ?, ?);" % sys.name
    sql_2 = "insert into v%sr values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);" % sys.name
    cur.executemany(sql_0, inns_pump_gen(prDate, inns, inns_spr))
    con.commit()
    print('инн закачены в базу', flush=True)
    #cur.execute('begin transaction')
    for values in gen1:
        gen2 = prices_pump(values[0], values[1], inns_spr)
        cur.executemany(sql_1, gen2)
        cur.execute(sql_2, values[1:])
    con.commit()

def prices_pump(prices, code, inns_spr):
    maxprice = 0.0
    minprice = 0.0
    inns_l = inns_spr.copy()
    while inns_l:
        pr_item = inns_l.pop(0)
        price = float(prices[pr_item[1]]/100)
        pr_name = pr_item[2]
        yield code, pr_name, price, maxprice, minprice

def inns_pump_gen(prDate, inns, inns_spr):
    inn_s = list(inns.items())
    while inn_s:
        inn, col_n = inn_s.pop(0)
        addr = f'org_{inn}'
        if inn == sys.name:
            name = 'PRICE'
        else:
            name = f'name_{inn}'
        inns_spr.append((inn, col_n, name))
        yield inn, inn, addr, name, 1, prDate

def main_pump(filename):
    load = init_load_bz2(filename)
    s = load(1)
    _rows = None
    if s[-2:] in [']]', '}]']:
        _rows = json.loads(s)
        inns = _rows.pop(0)
    else:
        inns = json.loads(s[1:])
    del s
    if _rows:
        row = _rows.pop(0)
    else:
        row = load(1)
    if _rows:
        row2 = _rows.pop(0)
    else:
        row2 = json.loads(row[1:])
    prDate = row2[0]
    yield prDate, inns
    if inns and row:
        while row:
            try:
                if _rows:
                    row = _rows.pop(0)
                else:
                    row = json.loads(row[1:])
            except:
                if _rows:
                    row = _rows.pop(0)
                else:
                    row = load(1)
                continue
            prices = row[2]
            code = row[3]
            good = row[4][0]
            vendor = row[5][0]
            maxpack = row[6]
            rest = row[7]
            reestr = float(row[8]) if row[8] else 0.0
            life = row[9] if row[9] else "0000-00-00"
            rounding = int(row[10]) if row[10] else 1
            minzak = int(row[11]) if row[11] else 1
            nds = int(row[12])
            idspr = int(row[13]) if row[13] else -1
            src_good = row[4][2]
            src_vendor = row[5][1]
            if not idspr or -1 == idspr:
                idspr = get_id_spr(src_good, src_vendor, code)
            uid = ''
            barcode = ''
            yield prices, code, good, vendor, maxpack, rest, reestr, life, rounding, minzak, src_good, src_vendor, barcode, nds, idspr, uid


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


def init_load_bz2(filename):
    g = _open_load_bz2(filename)
    g.__next__()
    return g.send

def _open_load_bz2(filename):
    f = None
    try:
        f = bz2.BZ2File(filename, 'r')
        yield None
        for row in f:
            r = yield row
            if r is None:
                break
    except Exception as Err:
        print('ошибка во время открытия исходного файла', flush=True)
        sys.exit(0)
    finally:
        if f:
            f.close()
    yield None


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
