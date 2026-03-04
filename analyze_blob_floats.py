import gzip, struct, re

TARGET = 'Document/Body/batch/Series/EvalContext/ParamContext/EigenschaftenListe'

with gzip.open('/Users/dominicwelti/Documents/Single_lap_shear.zs2','rb') as f:
    data=f.read()

i=4
n=len(data)
stack=[]
blobs={}

while i<n:
    if data[i]==0xFF:
        if stack: stack.pop()
        i+=1
        continue
    ln=data[i]; i+=1
    if i+ln>n: break
    name=data[i:i+ln].decode('ascii',errors='ignore'); i+=ln
    if i>=n: break
    dt=data[i]
    path='/'.join(stack)+'/'+name

    if dt==0xEE:
        i+=1
        if i+6>n: break
        sub=int.from_bytes(data[i:i+2],'little')
        cnt=int.from_bytes(data[i+2:i+6],'little'); i+=6
        bpi={0x4:4,0x5:8,0x16:4,0x11:1}.get(sub,0)
        need=cnt*bpi
        blob=data[i:i+need] if i+need<=n else b''
        if TARGET in path and path.endswith('/QS_ValSetting') and sub==0x0011:
            m=re.search(r'/Elem(\d+)/',path)
            if m:
                blobs[int(m.group(1))]=blob
        i+=need
    elif dt in (0xAA,0x00):
        i+=1
        if i+4>n: break
        raw=int.from_bytes(data[i:i+4],'little'); i+=4
        need=(raw & 0x7fffffff)*2
        i+=need
    elif dt==0xDD:
        i+=1
        if i>=n: break
        l=data[i]; i+=1+l
        stack.append(name)
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

for elem in sorted([2,3,4,5,6,7,11,20,60,295,296,300,304]):
    b=blobs.get(elem)
    if not b:
        continue
    print(f'Elem{elem} len={len(b)}')
    cands=[]
    for off in range(0,len(b)-4):
        v=struct.unpack('<f',b[off:off+4])[0]
        if abs(v)>1e-9 and abs(v)<1e6 and not (v!=v):
            # exclude super tiny denormals common in binary junk
            if abs(v) > 1e-4:
                cands.append((off,v))
    # keep first 20 unique rounded
    seen=set(); out=[]
    for off,v in cands:
        key=round(v,6)
        if key in seen: continue
        seen.add(key)
        out.append((off,v))
        if len(out)>=20: break
    print('  float cands:',out)
    ints=[]
    for off in range(0,len(b)-4):
        u=struct.unpack('<I',b[off:off+4])[0]
        if 1<=u<=100000 and off>12:
            ints.append((off,u))
    print('  int cands:',ints[:20])
