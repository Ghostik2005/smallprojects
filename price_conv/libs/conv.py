# coding: utf-8

import sys
import subprocess
import zipfile
import glob
import os
import libs.xlrd as xlrd
import time
import datetime

def puls_parse(filename, t_path):
    ch_time = os.stat(filename).st_mtime
    data = None
    with open(filename, 'rb') as f_obj:
        data = f_obj.read().decode('cp1251').replace(';', '\t')
    header = 'КОД\tF2\tТОВАР\tF4\tЦЕНА\tЗАВОД\tОСТАТОК\tУПАК\tF9\tF10\tF11\tЖНВЛС\tF13\tF14\tШТРИХ\tКРАТЗАК\tГОДЕН'
    data = data.split('\n')
    data.insert(0, header)
    return '\n'.join(data), ch_time

def protek_parse(filename, t_path):
    ch_time = os.stat(filename).st_mtime
    t_data = None
    with open(filename, 'rb') as f_obj:
        t_data = f_obj.read().decode('cp1251').replace(';', '\t')
    t_data = t_data.split('\n')
    pr_date = t_data[4].split('\t', 1)[0]
    pr_date = pr_date.split('.')
    pr_date = list(map(lambda x: int(x), pr_date))
    header = 'КОД\tТОВАР\tЗАВОД\tУПАК\tОСТАТОК\tЦЕНА\tГОДЕН\tКРАТЗАК\tМИНЗАК\tШТРИХ'
    t_data = t_data[8:]
    t_data.insert(0, header)
    time_tuple = ()
    ch_time = time.mktime((pr_date[-1], pr_date[-2], pr_date[-3], 0, 0, 0, 0, 0, 0))
    return '\n'.join(t_data), ch_time

def sia_parse(filename, t_path):
    ch_time = os.stat(filename).st_mtime
    t_data = None
    with zipfile.ZipFile(filename, 'r') as myzip:
        z_list = myzip.namelist()
        for z_file in z_list:
            myzip.extract(z_file, t_path)
    for f_name in glob.glob(os.path.join(t_path, '*.dbf')):
        f_txt = f_name.replace('.dbf', '.txt')
        cmd = f'python2.7 {t_path}/libs/dbf2csv.py -f {f_name} -o {f_txt} -d ";"'
        subprocess.call(cmd, shell=True)
        with open(f_txt, 'rb') as f_obj:
            t_data = f_obj.read().decode('cp866').replace(';', '\t')
        os.remove(f_txt)
        os.remove(f_name)
    t_data = t_data.split('\n')
    header = 'КОД\tТОВАР\tЗАВОД\tЦЕНА\tГОДЕН\tОСТАТОК\tУПАК\tМИНЗАК\tШТРИХ'
    t_data = t_data[1:]
    t_data.insert(0, header)
    return '\n'.join(t_data), ch_time

def katren_parse(filename, t_path):
    ch_time = os.stat(filename).st_mtime
    t_data = None
    with zipfile.ZipFile(filename, 'r') as myzip:
        z_list = myzip.namelist()
        for z_file in z_list:
            myzip.extract(z_file, t_path)
    for f_name in glob.glob(os.path.join(t_path, '*.txt')):
        with open(f_name, 'rb') as f_obj:
            t_data = f_obj.read().decode('cp1251').replace('|', '\t')
        os.remove(f_name)
    t_data = t_data.split('\n')
    header = 'F1\tF2\tКОД\tТОВАР\tЗАВОД\tЦЕНА\tЖНВЛСF7\tОСТАТОК\tШТРИХ\tГОДЕН\tКРАТЗАК'
    t_data.insert(0, header)
    return '\n'.join(t_data), ch_time

def mk_parse(filename, t_path):
    ch_time = os.stat(filename).st_mtime
    rb = xlrd.open_workbook(filename, formatting_info=True)
    sheet = rb.sheet_by_index(0)
    t_data = []
    for rownum in range(sheet.nrows):
        row = sheet.row_values(rownum)
        t_row = []
        for c_el in row:
            t_row.append(str(c_el))
        t_row = '\t'.join(t_row)
        t_data.append(t_row)
    header = 'F1\tF2\tТОВАР\tЗАВОД\tГОДЕН\tF6\tЦЕНА\tОСТАТОК\tF9\tF10\tF11\tF12'
    t_data = t_data[1:]
    t_data.insert(0, header)
    return '\n'.join(t_data), ch_time
