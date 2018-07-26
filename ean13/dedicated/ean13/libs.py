# coding: utf-8

import sys, os
sys.path.insert(0, '/ms71/data/ean13')
sys.path.insert(0, '/ms71/mini/lib/python3.6/site-packages')
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO as StringIO
import zlib
from datetime import datetime
import struct

# EAN13 Specs (all sizes in mm)
EDGE = '101'
MIDDLE = '01010'
CODES = {
    'A': ('0001101', '0011001', '0010011', '0111101', '0100011',
          '0110001', '0101111', '0111011', '0110111', '0001011'),
    'B': ('0100111', '0110011', '0011011', '0100001', '0011101',
          '0111001', '0000101', '0010001', '0001001', '0010111'),
    'C': ('1110010', '1100110', '1101100', '1000010', '1011100',
          '1001110', '1010000', '1000100', '1001000', '1110100'),
}
LEFT_PATTERN = ('AAAAAA', 'AABABB', 'AABBAB', 'AABBBA', 'ABAABB',
                'ABBAAB', 'ABBBAA', 'ABABAB', 'ABABBA', 'ABBABA')

class EAN13(object):
    name = 'EAN-13'
    #digits = 13
    digits = 12

    def __init__(self, ean=None):
        if (ean):
            self._set_ean(ean)
        else:
            self.ean = None

    def _set_ean(self, _ean):
        ean = str(_ean)
        ean = ean[:self.digits]
        if not ean.isdigit():
            raise ValueError('Code can only contain numbers.')
        #self.ean = ean
        self.ean = '{0}{1}'.format(ean, calculate_checksum(ean))

    def __unicode__(self):
        return self.ean

    __str__ = __unicode__

    def to_ascii(self, ean=None):
        """Builds the barcode pattern from `self.ean`.
        :returns: The pattern as string
        :rtype: String
        """
        if (ean):
            self._set_ean(ean)
        code = EDGE[:]
        pattern = LEFT_PATTERN[int(self.ean[0])]
        for i, number in enumerate(self.ean[1:7]):
            code += CODES[pattern[i]][int(number)]
        code += MIDDLE
        for number in self.ean[7:]:
            code += CODES['C'][int(number)]
        code += EDGE
        return code

def calculate_checksum(ean12):
    """http://fpfe.mipt.ru/science_articles/primecod.html"""
    s1 = 0
    s2 = 0
    for i, s in enumerate(ean12):
        if divmod(13-i, 2)[1]:
            s1+=int(s)
        else:
            s2+=int(s)
        c1 = s2*3 + s1
        c2 = c1//10
        if c1/10-c2:
            c2+=1
    return '%s' % (c2*10-c1)

def datetime_to_pdfdate(dt):
    return dt.strftime("%Y%m%d%H%M%SZ")

class pdfdoc(object):

    def __init__(self, version=3, title=None, author='Manuscript Solution', creator='Manuscript Solution',
                 producer='Manuscript Solution', creationdate=None, moddate=None, subject=None,
                 keywords=None, nodate=False):
        self.version = version # default pdf version 1.3
        now = datetime.now()
        self.objects = []
        info = {}
        if title:
            info[b"/Title"] = b"("+title.encode()+b")"
        if author:
            info[b"/Author"] = b"("+author.encode()+b")"
        if creator:
            info[b"/Creator"] = b"("+creator.encode()+b")"
        if producer:
            info[b"/Producer"] = b"("+producer.encode()+b")"
        if creationdate:
            info[b"/CreationDate"] = b"(D:"+datetime_to_pdfdate(creationdate).encode()+b")"
        elif not nodate:
            info[b"/CreationDate"] = b"(D:"+datetime_to_pdfdate(now).encode()+b")"
        if moddate:
            info[b"/ModDate"] = b"(D:"+datetime_to_pdfdate(moddate).encode()+b")"
        elif not nodate:
            info[b"/ModDate"] = b"(D:"+datetime_to_pdfdate(now).encode()+b")"
        if subject:
            info[b"/Subject"] = b"("+subject.encode()+b")"
        if keywords:
            info[b"/Keywords"] = b"("+b",".join(keywords)+b")"
        self.info = obj(info)
        # create an incomplete pages object so that a /Parent entry can be
        # added to each page
        self.pages = obj({
            b"/Type": b"/Pages",
            b"/Kids": [],
            b"/Count": 0
        })
        self.catalog = obj({
            b"/Pages": self.pages,
            b"/Type": b"/Catalog"
        })
        self.addobj(self.catalog)
        self.addobj(self.pages)

    def addobj(self, obj):
        newid = len(self.objects)+1
        obj.identifier = newid
        self.objects.append(obj)

    def addimage(self, color, width, height, imgformat, imgdata, pdf_x, pdf_y):
        if color == 'L':
            colorspace = b"/DeviceGray"
        elif color == 'RGB':
            colorspace = b"/DeviceRGB"
        elif color == 'CMYK' or color == 'CMYK;I':
            colorspace = b"/DeviceCMYK"
        else:
            sys.exit(1)
        # either embed the whole jpeg or deflate the bitmap representation
        if imgformat is "JPEG":
            ofilter = [ b"/DCTDecode" ]
        elif imgformat is "JPEG2000":
            ofilter = [ b"/JPXDecode" ]
            self.version = 5 # jpeg2000 needs pdf 1.5
        else:
            ofilter = [ b"/FlateDecode" ]
        image = obj({
            b"/Type": b"/XObject",
            b"/Subtype": b"/Image",
            b"/Filter": ofilter,
            b"/Width": width,
            b"/Height": height,
            b"/ColorSpace": colorspace,
            b"/BitsPerComponent": 8,
            b"/Length": len(imgdata)
        }, imgdata)
        if color == 'CMYK;I':
            # Inverts all four channels
            image.content[b'/Decode'] = [1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0]
        text = ("q\n%0.4f 0 0 %0.4f 0 0 cm\n/Im0 Do\nQ"%(pdf_x, pdf_y)).encode()
        content = obj({
            b"/Length": len(text)
        }, text)
        page = obj({
            b"/Type": b"/Page",
            b"/Parent": self.pages,
            b"/Resources": {
                b"/XObject": {
                    b"/Im0": image
                }
            },
            b"/MediaBox": [0, 0, pdf_x, pdf_y],
            b"/Contents": content
        })
        self.pages.content[b"/Kids"].append(page)
        self.pages.content[b"/Count"] += 1
        self.addobj(page)
        self.addobj(content)
        self.addobj(image)

    def tostring(self):
        # add info as last object
        self.addobj(self.info)
        xreftable = list()
        result = ("%%PDF-1.%d\n"%self.version).encode()
        xreftable.append(b"0000000000 65535 f \n")
        for o in self.objects:
            xreftable.append(("%010d 00000 n \n"%len(result)).encode())
            result += o.tostring()
        xrefoffset = len(result)
        result += b"xref\n"
        result += (b"0 %d\n"%len(xreftable))
        for x in xreftable:
            result += x
        result += b"trailer\n"
        result += parse({b"/Size": len(xreftable), b"/Info": self.info, b"/Root": self.catalog})+b"\n"
        result += b"startxref\n"
        result += b"%d\n"%xrefoffset
        result += b"%%EOF\n"
        return result

class obj(object):
    def __init__(self, content, stream=None):
        self.content = content
        self.stream = stream

    def tostring(self):
        if self.stream:
            return (
                ("%d 0 obj " % self.identifier).encode() +
                parse(self.content) +
                b"\nstream\n" + self.stream + b"\nendstream\nendobj\n"
                )
        else:
            rret = (b"%d 0 obj "%self.identifier)+parse(self.content)+b" endobj\n"
            return rret

def parse(cont, indent=1):
    if isinstance(cont, dict):
        mmm = sorted(cont.items())
        dd = list(4 * indent * b" " + k + b" " + parse(v, indent+1) for k, v in mmm)
        stri = b"\n".join(dd)
        reet = b"<<\n"+ stri + b"\n" + 4*(indent-1)*b" " + b">>"
        return reet
    elif isinstance(cont, int):
        return str(cont).encode()
    elif isinstance(cont, float):
        return ("%0.4f"%cont).encode()
    elif isinstance(cont, obj):
        reeer = ("%d 0 R"%cont.identifier).encode()
        return reeer
    elif isinstance(cont, str) or isinstance(cont, bytes):
        return cont
    elif isinstance(cont, list):
        return b"[ "+b" ".join([parse(c, indent) for c in cont])+b" ]"
    else:
        raise Exception("cannot handle type %s"%type(cont))

def bc30x20(title=u"Товар мазь крем для рук Мезим фортеТовар мазь крем для рук Мезим форте",
         ean="2000000000000", cnt=1, price=120.55, full_info=None, method=None):
    aTitle = title
    aEan = ean
    aPrice = price
    tovar_label1 = ""
    title = title.split('~')
    if len(title)>1:
        tovar_label1 = title.pop(0)
    title = title[0]
    try:
        _ean = EAN13(ean)
        ean = _ean.ean
        text = _ean.to_ascii()
    except:
        _ean = EAN13('9999999999999')
        ean = _ean.ean
        text = _ean.to_ascii()
        title = u"ОШИБКА: " + title
        cnt = 1
    pp = '/ms71/data/ean13'
    fnt1 = ImageFont.truetype(font=os.path.join(pp, 'PTN77F.ttf'), size=19)
    fnt12 = ImageFont.truetype(font=os.path.join(pp, 'PTN57F.ttf'), size=21)
    fnt123 = ImageFont.truetype(font=os.path.join(pp, 'PTN57F.ttf'), size=30) #####
    fnt2 = ImageFont.truetype(font=os.path.join(pp, 'PTN77F.ttf'), size=30)
    fnt245 = ImageFont.truetype(font=os.path.join(pp, 'PTN77F.ttf'), size=40)
    img = Image.new("1", (240, 160), 255)
    drw = ImageDraw.Draw(img)
    if title and title[0] != ' ':
        title = ' ' + title
    if 'big' == method or 'bigbold' == method:
        s = title[:20]; title = title[20:]
    else:
        s = title[:30]; title = title[30:]
    if s:
        if 'big' == method:
            drw.text((3,3), s, font=fnt123)
        elif 'bigbold' == method:
            drw.text((3,3), s, font=fnt2)
        else:
            drw.text((3,3), s, font=fnt1)
    if 'big' == method or 'bigbold' == method:
        s = title[:20]; title = title[20:]
    else:
        s = title[:30]; title = title[30:]
    if s:
        if 'big' == method:
            drw.text((3,17+12), s, font=fnt123)
        elif 'bigbold' == method:
            drw.text((3,17+12), s, font=fnt2)
        else:
            drw.text((3,17+12), s, font=fnt1)

    bars(text, drw)
    drw.text((10,119-15), ean[0], font=fnt12)
    drw.text((42,119-15), ean[1:7], font=fnt12)
    drw.text((134,119-15), ean[7:], font=fnt12)

    # Подвал: цена
    cena_label1 = u"Цена"
    cena_label2 = ""  # u"2906"
    cena_label3 = ""  # u"15"
    _price = (u"%s" % price).split('~')
    if len(_price) > 1:
        cena_label1 = _price.pop(0)
        if (not cena_label1) or tovar_label1:
            cena_label1 = u"Цена"
        price = _price.pop()
        cena_label2 = _price.pop(0) if _price else u""
        cena_label3 = _price.pop(0) if _price else u""
    else:
        price = _price[0]
    cena_value = u"".join(price.split()).replace(',', '-').replace('.', '-')
    lw1, lh1 = drw.textsize(cena_label1, font=fnt12)
    x, y = (3, 120+((50-lh1)//2) - 3)
    drw.text((x, y), cena_label1, font=fnt12)
    vw, vh = drw.textsize(cena_value, font=fnt245)
    vx, vy = ((240-vw)//2, 120+((50-vh)//2) - 4)
    drw.text((vx, vy), cena_value, font=fnt245)

    #if cena_label2:
        #lw2, lh2 = drw.textsize(cena_label2, font=fnt12)
        #x, y = (240-3-lw2, 120+((50-lh2)//2) + 3)
        #drw.text((x, y), cena_label2, font=fnt12)
    #if cena_label3:
        #lw3, lh3 = drw.textsize(cena_label3, font=fnt12)
        #x, y = (240-3-lw3, 120+((50-lh2-lh3)//2) - 8)
        #drw.text((x, y), cena_label3, font=fnt12)
    #img.save(f'/ms71/temp/ean13/ean13_b.png')

    if tovar_label1 and cena_label1:
        if full_info:
            for img_tag, cc in bc30x20tagfull(aTitle, aEan, 1, aPrice, full_info):
                yield img_tag
        else:
            for img_tag, cc in bc30x20tag(aTitle, aEan, 1, aPrice):
                yield img_tag
    for i in range(1 if not cnt else int(cnt)):
        yield img

def bc30x20tag(title=u"Товар мазь крем для рук Мезим фортеТовар мазь крем для рук Мезим фортеТовар мазь крем для рук Мезим форте",
         ean="2000000000000", cnt=1, price=0.00):
    tovar_label1 = ""
    title = title.split('~')
    if len(title)>1:
        tovar_label1 = title.pop(0)
    title = title[0].upper()
    # Проверяем: задано ли в начале количество повторений
    if tovar_label1:
        tovar_label1 = tovar_label1.split('|')
        if len(tovar_label1)>1:
            try: cnt = int(tovar_label1.pop(0))
            except Exception as Err:
                print(Err)
        tovar_label1 = tovar_label1[0]
    pp = '/ms71/data/ean13'
    #fnt1 = ImageFont.truetype(font=os.path.join(pp, 'PTN77F.ttf'), size=19, layout_engine='LAYOUT_BASIC')
    #fnt12 = ImageFont.truetype(font=os.path.join(pp, 'PTN57F.ttf'), size=21, layout_engine='LAYOUT_BASIC')
    #fnt2 = ImageFont.truetype(font=os.path.join(pp, 'PTN77F.ttf'), size=45, layout_engine='LAYOUT_BASIC')

    fnt1 = ImageFont.truetype(font=os.path.join(pp, 'RB.ttf'), size=16, layout_engine='LAYOUT_BASIC')
    fnt12 = ImageFont.truetype(font=os.path.join(pp, 'RR.ttf'), size=13, layout_engine='LAYOUT_BASIC')
    fnt2 = ImageFont.truetype(font=os.path.join(pp, 'RB.ttf'), size=42, layout_engine='LAYOUT_BASIC')
    
    img = Image.new("1", (240, 160), 255)
    drw = ImageDraw.Draw(img)
    if tovar_label1:
        if tovar_label1[0] != ' ':
            tovar_label1 = ' ' + tovar_label1
        drw.text((3,3), tovar_label1, font=fnt1)
    s = title[:24]; title = title[24:].strip()
    if s:
        drw.text((3,3 +25), s, font=fnt1)
    s = title[:24]; title = title[24:].strip()
    if s:
        drw.text((3,15+7 +25), s, font=fnt1)
    s = title[:24]; title = title[24:].strip()
    if s:
        drw.text((3,15+26 +25), s, font=fnt1)
    # Подвал: цена
    cena_label1 = ""
    cena_label2 = ""  # u"2906"
    cena_label3 = u"      Подпись"
    _price = (u"%s" % price).split('~')
    if len(_price) > 1:
        cena_label1 = _price.pop(0)
        #if not cena_label1:
            #cena_label1 = u""
        #else:
            #cena_label1 += cena_label3
        price = _price.pop()
        cena_label2 = _price.pop(0) if _price else u""
    else:
        price = _price[0]
    cena_value = u"".join(price.split()).replace(',', '-').replace('.', '-')
    vw, vh = drw.textsize(cena_value, font=fnt2)
    vx, vy = ((240-vw)//2, 120+((50-vh)//2) - 13 - 30)
    drw.text((vx, vy), cena_value, font=fnt2)
    #if cena_label3:
        ##lw3, lh3 = drw.textsize(cena_label3, font=fnt12)
        ##x, y = (240-3-lw3, 120+((50-lh2-lh3)//2) - 8)
        ##drw.text((x, y), cena_label3, font=fnt12)
        #pass
    if cena_label2:
        #c = cena_label1 + '    ' + cena_label2 + cena_label3
        #cena_label1 = c[0:39]
        #cena_label2 = c[39:]
        cena_label1 += cena_label3
        lw1, lh1 = drw.textsize(cena_label1, font=fnt12)
        x, y = (6, 121+((50-lh1)//2) - 0)
        drw.text((x, y), cena_label1, font=fnt12)
        lw2, lh2 = drw.textsize(cena_label2, font=fnt12)
        x, y = (3, 106+((50-lh2)//2) + 3)
        drw.text((x, y), cena_label2, font=fnt12)
    elif cena_label1:
        cena_label1 += cena_label3
        lw1, lh1 = drw.textsize(cena_label1, font=fnt12)
        x, y = (3, 112+((50-lh1)//2) - 0)
        drw.text((x, y), cena_label1, font=fnt12)
    #img.save(f'/ms71/temp/ean13/ean13_.png')
    if not cnt:
        cnt = 1
    for i in range(int(cnt)):
        yield img, cnt

def bc30x20tagfull(title=u"Товар мазь крем для рук Мезим фортеТовар мазь крем для рук Мезим фортеТовар мазь крем для рук Мезим форте",
         ean="2000000000000", cnt=1, price=0.00, full_info=None):
    footer = []
    for i in full_info:
        footer.append(i)
    tovar_label1 = ""
    title = title.split('~')
    if len(title)>1:
        tovar_label1 = title.pop(0)
    title = title[0].upper()
    # Проверяем: задано ли в начале количество повторений
    if tovar_label1:
        tovar_label1 = tovar_label1.split('|')
        if len(tovar_label1)>1:
            try: cnt = int(tovar_label1.pop(0))
            except Exception as Err:
                print(Err)
        tovar_label1 = tovar_label1[0]

    pp = '/ms71/data/ean13'
    fnt1 = ImageFont.truetype(font=os.path.join(pp, 'RB.ttf'), size=16, layout_engine='LAYOUT_BASIC')
    fnt12 = ImageFont.truetype(font=os.path.join(pp, 'RR.ttf'), size=13, layout_engine='LAYOUT_BASIC')
    fnt2 = ImageFont.truetype(font=os.path.join(pp, 'RB.ttf'), size=42, layout_engine='LAYOUT_BASIC')
    img = Image.new("1", (240, 160), 255)
    drw = ImageDraw.Draw(img)
    if tovar_label1:
        if tovar_label1[0] != ' ':
            tovar_label1 = ' ' + tovar_label1
        s1 = tovar_label1[:22].upper()
        ls11, hs11 = drw.textsize(s1, font=fnt12)
        drw.text((3,3), s1, font=fnt12)
    if len(title) > 46:
        s = title[:22] + " ..."
        drw.text((3, 23), s, font=fnt1)
        s = "... " + title[-22:]
        drw.text((3, 19+23), s, font=fnt1)
    else:
        s = title[:23]; title = title[23:].strip()
        if s:
            drw.text((3, 23), s, font=fnt1)
        s = title[:23]; title = title[23:].strip()
        if s:
            drw.text((3, 19+23), s, font=fnt1)
    l11, h11 = drw.textsize("ОБРАЗЕЦ", font=fnt12)
    drw.text((240-l11-3, 3), "ОБРАЗЕЦ", font=fnt12)
    _price = (u"%s" % price).split('~')
    if len(_price) > 1:
        price = _price.pop(1)
    else:
        price = _price[0]
    cena_value = u"".join(price.split()).replace(',', '-').replace('.', '-')
    vw, vh = drw.textsize(cena_value, font=fnt2)
    vx, vy = ((240-vw)//2, 50 + ((50-vh)//2) + 5)
    drw.text((vx, vy), cena_value, font=fnt2)

    try:
        label1 = footer[0]
        lw1, lh1 = drw.textsize(label1, font=fnt12)
        x, y = (3, (vy + vh//2) +((50-lh1)//2) + 9)
        drw.text((x, y), label1, font=fnt12)
    except:
        pass
    try:
        label2 = footer[1]
        lw2, lh2 = drw.textsize(label2, font=fnt12)
        x, y = (3, y + lh2 + 3)
        drw.text((x, y), label2, font=fnt12)
    except:
        pass
    try:
        label3 = footer[2]
        lw3, lh3 = drw.textsize(label3, font=fnt12)
        x, y = (3, y + lh3 + 3)
        drw.text((x, y), label3, font=fnt12)
    except:
        pass

    #img.save(f'/ms71/temp/ean13/ean13.png')
    for i in range(1 if not cnt else int(cnt)):
        yield img, cnt

def images(rows, method):
    delimiter = '\t'
    for row in rows:
        try:
            a = list(map(lambda x: x.strip(), row.strip().split(delimiter)))
            b = a[4:]
            a = a[:4]
        except Exception as e:
            a = []
            b = []
        if len(a) < 4:
            continue
        for img in bc30x20(*a, full_info=b, method=method):
            yield img

def images_ods(rows, method):
    delimiter = '\t'
    for row in rows:
        try:
            a = list(map(lambda x: x.strip(), row.strip().split(delimiter)))
            b = a[4:]
            a = a[:4]
        except Exception as e:
            a = []
            b = []
        if len(a) < 4:
            continue
        for img in bc30x20(*a, full_info=b, method=method):
            yield img, 1

def bars(text, drw):
    x0 = 25
    x1,y1, x2,y2 = (-2,72-12, 0,114-15)
    for ch in text[:3]:
        x1 = x1 + 1
        x2 = x1 + 1
        if "1" == ch:
            drw.rectangle((x0+x1,y1, x0+x2,y2+15), fill=0)
        x1 = x1 + 1
        x2 = x1 + 1
    for ch in text[3:46]:
        x1 = x1 + 1
        x2 = x1 + 1
        if "1" == ch:
            drw.rectangle((x0+x1,y1, x0+x2,y2), fill=0)
        x1 = x1 + 1
        x2 = x1 + 1
    for ch in text[46:49]:
        x1 = x1 + 1
        x2 = x1 + 1
        if "1" == ch:
            drw.rectangle((x0+x1,y1, x0+x2,y2+15), fill=0)
        x1 = x1 + 1
        x2 = x1 + 1
    for ch in text[-46:-3]:
        x1 = x1 + 1
        x2 = x1 + 1
        if "1" == ch:
            drw.rectangle((x0+x1,y1, x0+x2,y2), fill=0)
        x1 = x1 + 1
        x2 = x1 + 1
    for ch in text[-3:]:
        x1 = x1 + 1
        x2 = x1 + 1
        if "1" == ch:
            drw.rectangle((x0+x1,y1, x0+x2,y2+15), fill=0)
        x1 = x1 + 1
        x2 = x1 + 1

