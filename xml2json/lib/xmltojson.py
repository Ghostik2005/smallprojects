#coding: utf-8

__version__ = '2017.181.1030' # парсер xml -> json
from xml.parsers import expat
import json
import sys
import os.path

def main(arg=None):

    try:
        arg = arg.decode()
    except Exception as Err:
        #print(Err)
        pass
    nn = 'temp'
    if os.path.exists(arg):
        nn = os.path.basename(arg).split('.xml')[0]
        with open(arg, "rb") as i_file :
            arg = i_file.read().decode()
    elif hasattr(arg, 'read'):
        arg = arg.read()
        try:
            arg = arg.decode()
        except Ecxeption as Err:
            print(Err)

    if arg.startswith('-----'):
        arg = arg.split('\r')[-3].strip().encode()
    data_dict = parse(arg)
    for k in data_dict['КоммерческаяИнформация']['Документ']:
        pass
        #print(k, data_dict['КоммерческаяИнформация']['Документ'][k], sep='  <-->  ')
    for v in data_dict['КоммерческаяИнформация']['Документ']['Товары']['Товар']:
        pass
        #print(v)
    #print(data_dict['КоммерческаяИнформация']['Документ']['Товары']['Товар'])
    data = json.dumps(data_dict, ensure_ascii=False, indent=4) if arg else None
    if data:
        json_file = f'/ms71/data/xmltojson/{nn}.json'
        with open(json_file, "wb") as o_file:
            o_file.write(data.encode())
    return data

class _handler(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.elem_tree = []
        self.queue = []
        self.elem_data = []
        self.output = None
        self.attr_adding = True #обрабатываем, если нужны аттрибуты полей
        self.attr_prefix = '#' #символ перед именем аттрибута, чтоб не было конфликта если есть поля с таким же именем

    def field_header(self, fld_name, attribs):
        self.elem_tree.append((fld_name, attribs))
        self.queue.append((self.output, self.elem_data))
        if self.attr_adding:
            attrs_list = []
            for k, v in attribs.items():
                k = self.attr_prefix + k
                entry = (k, v)
                if entry:
                    attrs_list.append(entry)
            attribs = dict(attrs_list)
        else:
            attribs = None
        self.output = attribs
        self.elem_data = []

    def field_data(self, fld_name):
        if len(self.queue):
            data = None if not self.elem_data else self.elem_data[0]
            elem = self.output
            self.output, self.elem_data = self.queue.pop()
            if data:
                data = data.strip()
            if elem:
                self.output = self.save_data(self.output, fld_name, elem)
            else:
                self.output = self.save_data(self.output, fld_name, data)
        else:
            self.ouput = None
            self.elem_data = []
        self.elem_tree.pop()

    def char_data(self, data):
        self.elem_data.append(data)

    def save_data(self, elem, fld_name, data):
        if not elem:
            elem = dict()
        try:
            f_value = elem[fld_name]
            if isinstance(f_value, list): #если значение -  список, то к нему добавляем data
                f_value.append(data)
            else: #если словарь - то всталяем в словарь список
                elem[fld_name] = [f_value, data] 
        except KeyError as Err:
            #print(Err)
            elem[fld_name] = data
        return elem

def parse(xml_input, encoding='utf-8', **kwargs):

    handler = _handler(**kwargs)
    parser = expat.ParserCreate(encoding)
    parser.StartElementHandler = handler.field_header
    parser.EndElementHandler = handler.field_data
    parser.CharacterDataHandler = handler.char_data
    parser.ParseFile(xml_input) if hasattr(xml_input, 'read') else parser.Parse(xml_input)
    return handler.output


if "__main__" == __name__:
    main('/ms71/temp/xmltojson/parse/new.xml')
