# coding: utf-8

import sys, os
sys.path.insert(0, '/ms71/data/ean13')
sys.path.insert(0, '/ms71/mini/lib/python3.6/site-packages')
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO as StringIO
import zlib
from datetime import datetime
try:
    from ean13 import libs
except ImportError:
    import libs

def convert(images, title='Barcode', colorspace='1'):
    pdf = libs.pdfdoc(title=title)
    for imfilename in images:
        rawdata = None
        imgdata = None
        output = StringIO()
        imfilename.save(output, "PNG")
        try:
            rawdata = output.getvalue()
        except Exception as Err:
            print(Err)
            continue
        output.close()
        try:
            imgdata = Image.open(StringIO(rawdata))
        except IOError as Err:
            print(Err)
            continue
        else:
            width, height = imgdata.size
            imgformat = imgdata.format
            ndpi = imgdata.info.get("dpi", (203, 203))
            if colorspace:
                color = colorspace
            else:
                color = imgdata.mode
        if color == '1':
            imgdata = imgdata.convert('L')
            color = 'L'
        else:
            imgdata = imgdata.convert('RGB')
            color = imgdata.mode
        imgdata = zlib.compress(imgdata.tobytes())
        # pdf units = 1/72 inch
        pdf_x, pdf_y = 72.0*width/ndpi[0], 72.0*height/ndpi[1]
        pdf.addimage(color, width, height, imgformat, imgdata, pdf_x, pdf_y)
    return pdf.tostring()

def main2(title, rows, output, method):
    output.write(convert(libs.images(rows, method), title=title))

if __name__ == '__main__':
    c = 0
    
    for img in libs.bc30x20(title=u"1|АПТЕКА НИЗКИХ ЦЕН ООО~лонгидаза супп вагин рект 3000ме №10(россия)",
         ean="1000587752001", cnt=2, price="с/г: 01.06.2019~серия: 45062017 05.03.2018~1874,00"):
         #ean="1000587752001", cnt=2, price="с/г: 01.06.2019~1874,00"):
        c += 1
        img.save('testV2-%stag_.png' % c)
