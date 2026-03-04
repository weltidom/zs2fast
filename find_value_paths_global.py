import gzip, re, struct

pat = re.compile(r'wert|value|val|result|ergeb|textpar|valpar', re.I)

with gzip.open('/Users/dominicwelti/Documents/Single_lap_shear.zs2', 'rb') as f:
    data = f.read()

i=4
n=len(data)
stack=[]
hits=[]

while i<n and len(hits)<1200:
    if data[i]==0xFF:
        if stack: stack.pop()
        i+=1
        continue

    name_len=data[i]; i+=1
    if i+name_len>n: break
    name=data[i:i+name_len].decode('ascii', errors='ignore'); i+=name_len
    if i>=n: break
    dtype=data[i]
    path='/'.join(stack)+'/'+name

    matched = pat.search(path) is not None

    if dtype in (0xAA,0x00):
        i+=1
        if i+4>n: break
        raw=int.from_bytes(data[i:i+4],'little'); i+=4
        need=(raw & 0x7fffffff)*2
        if i+need>n: break
        txt=data[i:i+need].decode('utf-16le', errors='ignore').rstrip('\x00')
        if matched:
            hits.append((path,'text',txt))
        i+=need
    elif dtype==0xEE:
        i+=1
        if i+6>n: break
        sub=int.from_bytes(data[i:i+2],'little')
        cnt=int.from_bytes(data[i+2:i+6],'little')
        i+=6
        bpi={0x4:4,0x5:8,0x16:4,0x11:1}.get(sub,0)
        need=cnt*bpi
        prev=None
        if i+need<=n and cnt>0 and bpi>0:
            if sub==0x4: prev=struct.unpack('<f',data[i:i+4])[0]
            elif sub==0x5: prev=struct.unpack('<d',data[i:i+8])[0]
            elif sub==0x16: prev=struct.unpack('<I',data[i:i+4])[0]
            elif sub==0x11: prev=data[i]
        if matched:
            hits.append((path,f'EE{sub:04X}[{cnt}]',prev))
        if i+need>n: break
        i+=need
    elif dtype==0xDD:
        i+=1
        if i>=n: break
        dd_len=data[i]
        i+=1+dd_len
        stack.append(name)
        if matched:
            hits.append((path,'section',f'dd_len={dd_len}'))
    elif dtype in (0x11,0x22,0x33,0x44):
        i+=1
        if i+4>n: break
        raw4=data[i:i+4]
        if dtype==0x11: val=raw4[0]
        elif dtype==0x22: val=struct.unpack('<i',raw4)[0]
        elif dtype==0x33: val=struct.unpack('<I',raw4)[0]
        else: val=struct.unpack('<f',raw4)[0]
        if matched:
            hits.append((path,{0x11:'u8',0x22:'i32',0x33:'u32',0x44:'f32'}[dtype],val))
        i+=4
    elif dtype in (0x55,0x66):
        i+=3
    elif dtype in (0x88,0x99):
        i+=2
    elif dtype==0xBB:
        i+=5
    elif dtype==0xCC:
        i+=1
        if i+8>n: break
        val=struct.unpack('<d',data[i:i+8])[0]
        if matched:
            hits.append((path,'f64',val))
        i+=8
    else:
        i+=1

print('hits',len(hits))
for p,t,v in hits[:1000]:
    print(f'{t:12} {p}\n  -> {v}')
