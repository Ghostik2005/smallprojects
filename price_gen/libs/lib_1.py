#coding: utf-8

import os
import sys
import time
import json
import glob
from io import BytesIO
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import ms71lib.client as ms71_cli
import libs

class price(object):

    def __init__(self, name='Организация'):
        self.name = name
        self.rows = []
        self.x = 6.5
        self.ret_object = BytesIO()
        self.properties = {
            'title':    'PL',
            #'#author':   'M S',
            #'company':  'M S',
            'category': 'Utility',
            #'comments': 'Created with Python and modified xlsxwriter from clickhouse'
                }
        self.workbook = libs.Workbook(self.ret_object, {'in_memory': True})
        self.workbook.set_properties(self.properties)
        self.cell_format = {}
        self.cell_format['header'] = self.workbook.add_format({'font_size': '8', 'bold': True, 'align': 'center', 'bottom': 1})
        self.cell_format['row'] = self.workbook.add_format({'font_size': '8', 'align': 'left'})

    def clear_rows(self):
        self.rows.clear()

    def add_row(self, code ='Код', price='Цена', tovar='Товар', vendor='Производитель', rest='Остаток'):
        row = []
        row.append(code)
        row.append(price)
        row.append(tovar)
        row.append(vendor)
        row.append(rest)
        self.rows.append(row)

    def add_sheet(self, supplier='Поставщик'):
        worksheet = self.workbook.add_worksheet(supplier)
        worksheet.set_print_scale(100)
        j = 0
        max_widths = {}
        while self.rows:
            row = self.rows.pop(0)
            for i in range(len(row)):
                data = row[i]
                l = len(str(data))
                if j == 0:
                    max_widths[i] = l
                if max_widths[i] < l:
                    max_widths[i] = l
            worksheet.write_row(j, 0, row, self.cell_format['header' if j==0 else 'row'])
            j += 1
        for i in max_widths:
            worksheet.set_column(i, i, max_widths[i]*(self.x if max_widths[i] > 10 else 9))
        worksheet.set_paper(9) #размер А4
        worksheet.set_portrait() #портретная ориентация
        worksheet.set_margins(left=1, right=0.5, top=0.5, bottom=0.5)

    def generate_file(self):
        self.workbook.close()
        return self.ret_object



def main (params, wd=''):
    sql_pos = "select Inns.Inn, Inns.Poz from offer.head_{0} as h array join Inns.Inn as a where a = '{1}' and SuppId = '{0}' FORMAT JSONCompact"
    customer = params.pop("inn")
    suppl_list = []
    for k in params:
        if k.isdigit():
            suppl_list.append(k)
    suppl_list = tuple(suppl_list)
    serv_name = "https://sklad71.org/ch0/"
    api_key = "fb6153fe31944f1e81eb061127b8b3d3"
    server = ms71_cli.ServerProxy(serv_name, verbose=False, api_key=api_key, allow_none=True)  
    request = server("request")
    price_list = price(customer)
    for suppl in suppl_list:
        print('Обрабатывается-> ', suppl, flush=True)
        resp = request(sql_pos.format(suppl, customer).encode())['data']
        try:
            response = resp[0]
        except Exception as Err:
            response = None
            print(Err, flush=True)
        if customer == '7105507009':
            exclude = ['20557', '20377']
            if suppl in exclude:
                print('Пропускаем->', suppl, sep = '\t', flush=True)
                response = None
        price_list.add_row()
        if response:
            pos = response[1][response[0].index(customer)]
            gen = get_values(request, suppl, pos)
            for row in gen:
                row[1] = row[1]/100
                price_list.add_row(*row)
        price_list.add_sheet(supplier=params[f'{suppl}'])
        ##############
        #break
    print('Формируем XLSX файл', flush=True)
    f_obj = price_list.generate_file()
    data = f_obj.getvalue()
    f_obj.close()
    f_name = os.path.join(wd, f'{customer}.xlsx')
    with open(f_name, 'wb') as f_obj:
        f_obj.write(data)
    return f_name

def send_mail(m):
    ret_list = []
    if m['ssl']:
        from smtplib import SMTP_SSL as SMTP
    else:
        from smtplib import SMTP as SMTP
    f_list = glob.glob(os.path.join(m['tmp'], '*.mail'))
    while f_list:
        item = f_list.pop()
        i = os.path.basename(item)[:-5]
        with open(item, 'rb') as f_obj:
            to_list = f_obj.read().decode().split('\n')
        from_list = f'price generator <{m["login"]}>'
        msg = MIMEMultipart('mixed')
        msg['Subject'] = 'Прайс-лист'
        msg['From'] = from_list
        msg['To'] = ', '.join(to_list) 
        part1 = MIMEText('Прайс лист во вложении', 'plain')
        part2 = None
        file_name = os.path.join(m['tmp'], f'{i}.xlsx')
        with open(file_name, 'rb') as f_obj:
            part2 = MIMEApplication(f_obj.read(), f'octet-stream;name="{os.path.basename(file_name)}"')
        msg.attach(part1)
        if part2:
            msg.attach(part2)
        with SMTP(host=m['host'], port=m['port']) as smtp:
            smtp.login(m['login'], m['password'])
            smtp.sendmail(f'{m["login"]}', to_list, msg.as_string())
        try:
            #pass
            os.remove(file_name)
            os.remove(item)
        except Exception as Err:
            print(Err, flush=True)
        ret_list.append(i)
    return ret_list


def get_mails(mails):
    if '#' in mails:
        mails, _ = split('#', 1)
    if ',' in mails:
        mm = mails.split(',')
        mails = '\n'.join(x.strip() for x in mm)
    elif ';' in mails:
        mm = mails.split(',')
        mails = '\n'.join(x.strip() for x in mm)
    return mails

    
def get_values(request, suppl, pos):
    sql_part = """
select Kod, Ceny[{0}+1] as c, Tovar[3], Zavod[2], Ostatok from offer.body_{1}  where c > 0 and Ostatok > 0
    format JSONCompact
    """
    sql_t = sql_part.format(pos, suppl)
    #print(sql_t)
    response = request(sql_t.encode())
    for row in response['data']:
        yield row

if "__main__" == __name__:
    params = {'inn': '7106042317', '20171': 'сиа', '20277': 'протек', '20557': 'катрен'}
    main(params)


