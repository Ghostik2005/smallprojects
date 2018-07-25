# coding: utf-8

import sys, os, zipfile, tempfile
from io import BytesIO as StringIO
from datetime import datetime
try:
    from ean13 import libs
except ImportError:
    import libs

def tamplate_get(templatename, fg_cell=False):
    z=data0=row=data1=None
    if fg_cell:
        manifest = ""
        data = templatename
    else:
        z = zipfile.ZipFile(templatename, 'r')
        manifest = z.read('META-INF/manifest.xml')
        data = z.read('content.xml')
    i0 = data.decode().find('<table:table-cell' if fg_cell else '<table:table-row')
    if i0>-1:
        data0 = data[:i0]
        txt = '</table:table-cell>' if fg_cell else '</table:table-row>'
        i1 = data.decode().find(txt, i0) + len(txt)
        row = data[i0:i1]  # .replace('>ds<', '><')
        data1 = data[i1:]
    if z:
        z.close()
    return manifest, data0, row, data1

def report_init(templatename, output):
    g = _report_save(templatename, output)
    g.__next__()
    return g.send

def _report_save(templatename, output):
    zz, z = None, None
    f, reportname = tempfile.mkstemp('.ods', 'ean2ods-')
    os.close(f)
    try:
        zz = zipfile.ZipFile(reportname, 'w', zipfile.ZIP_DEFLATED)
        z = zipfile.ZipFile(templatename, 'r')
        for nm in z.namelist():
            if (nm not in ['content.xml', 'META-INF/manifest.xml']) and nm[-1] != '/':
                zz.writestr(nm, z.read(nm))
        if z:
            z.close()
            z = None
        while True:
            item = yield True
            if item is None:
                break
            zz.writestr(item[0], item[1])
        if zz:
            zz.close()
            zz = None
        yield open(reportname, 'rb').read()
    finally:
        if z:
            z.close()
        if zz:
            zz.close()
        try: os.remove(reportname)
        except: pass

def toods(title, rows=None, output=None, method=None):
    ret_output = output
    templatename = os.path.join('/ms71/data/ean13', 'bc30x20.a4.zip')
    manifest, data0, row, data1 = tamplate_get(templatename)
    _, row0, cell, row1 = tamplate_get(row, True)
    content = [data0,]
    img_count = 0
    row_index = 0
    col_list = ['A', 'B', 'C', 'D', 'E', 'F']
    c = 0
    pictures = []
    save = report_init(templatename, output)
    for img, cnt in libs.images_ods(rows, method):
        output = StringIO()
        img.save(output, "PNG")
        imgdata = output.getvalue()
        output.close()
        img_count += 1
        imgnm = "%s.png" % img_count
        save(["Pictures/%s" % imgnm, imgdata])
        pictures.append(' <manifest:file-entry manifest:media-type="image/png" manifest:full-path="Pictures/%s"/>' % imgnm)
        for i in range(cnt):
            if c % 6 == 0:
                row_index += 1
                content.append(row0)
            ca = "30x20.%s%s" % (col_list[c % 6], row_index)
            try: 
                cell = cell.decode()
            except Exception as Err:
                pass
            c1 = cell.replace("30x20.A1", ca).replace('1.png', imgnm)
            c1 = c1.encode()
            content.append(c1)
            c += 1
            if c % 6 == 0:
                content.append(row1)
    if c > 0 and c % 6 != 0:
        content.append(row1)
    content.append(data1)
    manifest = manifest.replace(pictures[0].encode(), ("\n".join(pictures)).encode())
    save(['META-INF/manifest.xml', manifest])
    content1 = "".join([i.decode() for i in content])
    save(['content.xml', content1])
    ret_output.write(
        save(None)
    )

if __name__ == '__main__':

    stri = "ods&ООО \"Любимая аптека\"~Товар мазь крем для рук Мезим форте Товар мазь крем для рук Мезим форте Товар мазь крем для рук Мезим форте;1007290004649;10;11.12.2018~123.45~2019~c/г мар.;с/г: 01.11.2019 серия 071117;714/07.03.18 ПРОТЕК ЗАО ЦВ Г.МОСКВА;88562382-001 от 06.03.2018"
    #stri = "ods&ООО \"Любимая аптека\"~Товар мазь крем для рук Мезим форте Товар мазь крем для рук Мезим форте Товар мазь крем для рук Мезим форте;1007290004649;1;11.12.2018~123.45~2019~c/г мар."
    stri = stri.replace(';', '\t').replace('&', '\n')
    output = StringIO()
    method = None
    toods("ean13", stri.splitlines(), output, method)
