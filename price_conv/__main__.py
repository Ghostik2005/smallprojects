# coding: utf-8

"""
Price converter for Arhangelsk region customer.
Supported suppliers:
    katren
    sia
    puls
    mk
    protek
"""

__appname__ = "price_converter"
__version__ = "17.208.1800" 

import os
import sys
import time
import glob
import shutil
import zipfile
import argparse
from libs import conv

def main():
    t_path, output_path, s_path = ARG_PARSE()
    ret = _pre(t_path, output_path, s_path)
    timestamp =time.strftime("%Y_%j_%H%M")
    if ret:
        for filename in get_names(path=s_path):
            filename = os.path.abspath(filename)
            data, ch_time, pr_name = price_process(filename=filename, t_path=t_path, timestamp=timestamp)
            if data:
                os.remove(filename)
                out_zip = os.path.join(output_path, pr_name)
                if os.path.exists(out_zip):
                    f_pathes = os.path.splitext(out_zip)
                    out_zip = ''.join([f_pathes[0], '_', str(int(time.time())), f_pathes[1]])
                out_f_name = os.path.basename(filename)[:-4]+'.txt'
                with zipfile.ZipFile(out_zip, 'w') as zip_f:
                    with zip_f.open(out_f_name, 'w') as out_f_obj:
                        out_f_obj.write(data.encode())
                print(os.path.basename(filename), '->>', out_zip, 'succesfully', sep='\t', flush=True)
                os.utime(out_zip, times=(ch_time, ch_time))
                time.sleep(1.1)
        _exit(t_path)
    else:
        print('we have not enough access rights...', flush=True)

def price_process(filename=None, t_path=None, timestamp=''):
    data = None
    pr_names = {
    'katren':f'20576_katren/price20576-2901161825_{timestamp}.zip',
    'sia':f'20129_sia/price20129-2901161825_{timestamp}.zip',
    'puls':f'28176_puls/price28176-2901161825_{timestamp}.zip',
    'mk':f'51070_mk/price51070-2901161825_{timestamp}.zip',
    'protek':f'20229_protek/price20229-2901161825_{timestamp}.zip'
    }
    if filename:
        if 'sia' in filename:
            data, ch_time = conv.sia_parse(filename, t_path)
            pr_name = pr_names['sia']
        elif 'katren' in filename:
            data, ch_time = conv.katren_parse(filename, t_path)
            pr_name = pr_names['katren']
        elif 'Puls' in filename:
            data, ch_time = conv.puls_parse(filename, t_path)
            pr_name = pr_names['puls']
        elif 'mk' in filename:
            data, ch_time = conv.mk_parse(filename, t_path)
            pr_name = pr_names['mk']
        elif '.sst' in filename:
            data, ch_time = conv.protek_parse(filename, t_path)
            pr_name = pr_names['protek']
        else:
            print(f'unknown supplier ->\t{filename}')
            data, ch_time, pr_name = None, None, None
    return data, ch_time, pr_name

def ARG_PARSE():
    desc = u'Price converter for Arhangelsk region customer. Supported suppliers: katren, sia, puls, mk, protek'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-o', '--output', help='output directory (default: %(default)s)', 
                        default='/ms71/data/price_converter',)
    parser.add_argument('-t', '--temp', help='temp directory (default: %(default)s)',
                        default = '/ms71/temp/price_converter')
    parser.add_argument('-s', '--source', help='source directory (default: %(default)s)',
                        default = '/ms71/data/prices_s')
    args = parser.parse_args()
    return (args.temp, args.output, args.source)

def _exit(t_path):
    shutil.rmtree(t_path)

def _pre(t_path, output_path, s_path):
    print('|working directories:', flush=True)
    print(f'||temp directory: {t_path}', flush=True)
    print(f'||output directory: {output_path}', flush=True)
    print(f'||source directory: {s_path}', flush=True)
    print(flush=True)
    print('results:', flush=True)
    pr_names = [
    f'{output_path}/20576_katren',
    #f'{output_path}/20129_sia',
    f'{output_path}/28176_puls',
    #f'{output_path}/51070_mk',
    #f'{output_path}/20229_protek'
    ]
    e_list = [
    'libs/__init__.py',
    'libs/dbf2csv.py',
    'libs/dbfpy/__init__.py',
    'libs/dbfpy/utils.py',
    'libs/dbfpy/dbf.py',
    'libs/dbfpy/header.py',
    'libs/dbfpy/record.py',
    'libs/dbfpy/dbfnew.py',
    'libs/dbfpy/fields.py',
    'libs/dbfpy/memo.py'
    ]
    ret = True
    try:
        try:
            os.makedirs(output_path, mode=0o777)
        except:
            pass
        for dd in pr_names:
            if not os.path.exists(dd):
                os.mkdir(dd)
        try:
            os.makedirs(t_path, mode=0o777)
        except:
            pass
        filename = os.path.abspath(sys.argv[0])
        if filename.endswith('.zip'):
            with zipfile.ZipFile(filename, 'r') as myzip:
                z_list = myzip.namelist()
                for z_file in e_list:
                    myzip.extract(z_file, t_path)
        else:
            shutil.copytree(os.path.join(os.path.dirname(filename), 'libs'), os.path.join(t_path, 'libs'))
    except:
        ret = False
    return ret

def get_names(path='.'):
    for root, dirs, files in os.walk(path+'/'):
        for file_n in files:
            f_name = os.path.join(root, file_n)
            yield f_name

if __name__ == '__main__':
    t1 = time.time()
    try:
        main()
    except Exception as Err:
        print(Err, flush=True)
    t2 = time.time()
    tt = time.strftime("Total executed time: %H:%M:%S", time.gmtime(t2 - t1))
    print(tt, flush=True)
    sys.exit()
