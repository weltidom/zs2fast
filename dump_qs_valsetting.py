import gzip, struct, re

TARGET_ROOT = 'Document/Body/batch/Series/EvalContext/ParamContext/EigenschaftenListe'

with gzip.open('/Users/dominicwelti/Documents/Single_lap_shear.zs2','rb') as f:
    data=f.read()

i=4
n=len(data)
stack=[]

hits=[]

while i<n:
    if data[i]==0xFF:
        if stack: stack.pop()
        i+=1
        continue

    name_len=data[i]; i+=1
    if i+name_len>n: break
    name=data[i:i+name_len].decode('ascii',errors='ignore'); i+=name_len
    if i>=n: break

    dtype=data[i]
    path='/'.join(stack)+'/'+name

    if dtype==0xEE:
        i+=1
        if i+6>n: break
        sub=int.from_bytes(data[i:i+2],'little')
        cnt=int.from_bytes(data[i+2:i+6],'little')
        i+=6
        bpi={0x4:4,0x5:8,0x16:4,0x11:1}.get(sub,0)
        need=cnt*bpi
        blob=data[i:i+need] if i+need<=n else b''

        if TARGET_ROOT in path and ('QS_ValSetting' in path or 'QS_TextPar' in path or 'QS_ValPar' in path):
            m=re.search(r'/Elem(\d+)/', path)
            elem=int(m.group(1)) if m else -1
            # decode attempts
            ascii_txt=''.join(chr(b) if 32<=b<127 else '.' for b in blob[:120])
            utf16_txt=''
            try:
                utf16_txt=blob.decode('utf-16le',errors='ignore').replace('\x00','')[:80]
            except:
                pass
            first_f32=struct.unpack('<f', blob[:4])[0] if len(blob)>=4 else None
            first_f64=struct.unpack('<d', blob[:8])[0] if len(blob)>=8 else None
            hits.append((elem,path.split('/')[-1],sub,cnt,blob[:40].hex(' '),ascii_txt,utf16_txt,first_f32,first_f64))

        if i+need>n: break
        i+=need
    elif dtype in (0xAA,0x00):
        i+=1
        if i+4>n: break
        raw=int.from_bytes(data[i:i+4],'little'); i+=4
        need=(raw & 0x7fffffff)*2
        if i+need>n: break
        i+=need
    elif dtype==0xDD:
        i+=1
        if i>=n: break
        l=data[i]; i+=1+l
        stack.append(name)
    elif dtype in (0x11,0x22,0x33,0x44):
        i+=5
    elif dtype in (0x55,0x66):
        i+=3
    elif dtype in (0x88,0x99):
        i+=2
    elif dtype==0xBB:
        i+=5
    elif dtype==0xCC:
        i+=9
    else:
        i+=1

hits.sort(key=lambda x:(x[0],x[1]))
print('hits',len(hits))
for h in hits[:220]:
    elem,field,sub,cnt,hex40,ascii_txt,utf16_txt,f32,f64=h
    print(f'Elem{elem} {field} sub={sub:04x} cnt={cnt}')
    print(' hex:',hex40)
    print(' ascii:',ascii_txt)
    print(' utf16:',utf16_txt)
    print(' f32/f64:',f32,f64)
