#!/usr/bin/env python
# -*- coding: utf8 -*-

import csv

import sys
from optparse import OptionParser
from dbfpy import dbf

def echo_err(parser,msg):
    parser.print_help()
    print "*** " + msg
    sys.exit(1)

def main ():
    usage = "usage: %prog --file=DBFfile --out=CSVfile --header=False|True --delim=DELIM --cols=COLLIST"
    parser = OptionParser(usage=usage)

    parser.add_option("-f", "--file", action="store", dest="dbfFile", help="dbf file")
    parser.add_option("-o", "--out", action="store", dest="csvFile", help='csv file')
    parser.add_option("-d", "--delim", action="store", dest="delim", help='delimiter')
    parser.add_option("", "--header", action="store", dest="header", help='header (True|False)')
    parser.add_option("", "--cols", action="store", dest="cols", help='list of columns. Exp: 1,2,6,9,11')

    (options, args) = parser.parse_args()
    
    dbfFile = options.dbfFile # входной файл
    csvFile = options.csvFile # выходной файл
    delim = options.delim # разделитель в выходном файле
    header = options.header # писать заголовки колонок в выходном файле?
    cols = options.cols # список колонок, которые нужно записать в выходной файл
    

    if delim == None:
        delim = ','
            
    if dbfFile == None:
        echo_err(parser,"DBF datafile is required")
    if csvFile == None:
        csvFile = csv.writer(sys.stdout,delimiter=delim)
    else:
        csvFile = csv.writer(open(csvFile, 'wt'),delimiter=delim)
    
    if header == None:
        header = True
    elif header == 'True':
        header = True
    elif header == 'False':
        header = False
    else:
        echo_err(parser, 'header=True|False')
    
    dbfFile = dbf.Dbf(dbfFile)
    if cols: 
        cols = [int(s)-1 for s in cols.split(',')]
        assert max(cols)<=len(dbfFile.fieldNames)-1
        assert min(cols)>=0
    else: # по умолчанию берем все колонки файла
        cols = range(len(dbfFile.fieldNames))
    
    if header:
        csvFile.writerow([dbfFile.fieldNames[i] for i in cols])    
    for row in dbfFile:
        fields = [row[num] for num in cols]
        csvFile.writerow(fields)


if __name__=="__main__":
    main()
