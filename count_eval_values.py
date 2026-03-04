import re
import gzip

with gzip.open('/Users/dominicwelti/Documents/Single_lap_shear.zs2','rb') as f:
    data=f.read()

i=4
n=len(data)
stack=[]
vals=[]

while i<n:
    if data[i]==0xFF:
        if stack:
            stack.pop()
        i+=1
        continue

    ln=data[i]
    i+=1
    if i+ln>n:
        break
    name=data[i:i+ln].decode('ascii',errors='ignore')
    i+=ln
    if i>=n:
        break

    dt=data[i]
    path='/'.join(stack)+'/'+name

    if dt in (0xAA,0x00):
        i+=1
        raw=int.from_bytes(data[i:i+4],'little')
        i+=4
        need=(raw & 0x7fffffff)*2
        if i+need>n:
            break
        txt=data[i:i+need].decode('utf-16le',errors='ignore').rstrip('\x00')
        i+=need

        if '/Evaluation/PrivateContext/EigenschaftenListe/' in path and path.endswith('/Name/Text'):
            m=re.search(r'/EigenschaftenListe/Elem(\d+)/', path)
            if m:
                vals.append(txt)

    elif dt==0xDD:
        i+=1
        if i>=n:
            break
        l=data[i]
        i+=1+l
        stack.append(name)
    elif dt==0xEE:
        i+=1
        sub=int.from_bytes(data[i:i+2],'little')
        cnt=int.from_bytes(data[i+2:i+6],'little')
        i+=6
        bpi={0x04:4,0x05:8,0x16:4,0x11:1}.get(sub,0)
        i+=cnt*bpi
    elif dt in (0x11,0x22,0x33,0x44):
        i+=5
    elif dt in (0x55,0x66):
        i+=3
    elif dt in (0x88,0x99):
        i+=2
    elif dt==0xBB:
        i+=5
    elif dt==0xCC:
        i+=9
    else:
        i+=1

num=[]
other=[]
for t in vals:
    s=t.strip().replace(',','.')
    try:
        float(s)
        num.append(t)
    except Exception:
        other.append(t)

print('total',len(vals),'numeric parse',len(num),'other',len(other))
print('numeric unique sample',sorted(set(num))[:40])
print('other unique sample',sorted(set(other))[:60])
