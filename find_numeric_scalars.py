import gzip, struct

with gzip.open('/Users/dominicwelti/Documents/Single_lap_shear.zs2','rb') as f:
    data=f.read()

i=4
n=len(data)
stack=[]
rows=[]

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

    if dtype==0x22:
        i+=1
        if i+4>n: break
        v=struct.unpack('<i',data[i:i+4])[0]; i+=4
        rows.append(('i32',path,v))
    elif dtype==0x33:
        i+=1
        if i+4>n: break
        v=struct.unpack('<I',data[i:i+4])[0]; i+=4
        rows.append(('u32',path,v))
    elif dtype==0x44:
        i+=1
        if i+4>n: break
        v=struct.unpack('<f',data[i:i+4])[0]; i+=4
        rows.append(('f32',path,v))
    elif dtype==0xCC:
        i+=1
        if i+8>n: break
        v=struct.unpack('<d',data[i:i+8])[0]; i+=8
        rows.append(('f64',path,v))
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
    elif dtype==0xEE:
        i+=1
        if i+6>n: break
        sub=int.from_bytes(data[i:i+2],'little'); cnt=int.from_bytes(data[i+2:i+6],'little'); i+=6
        bpi={0x4:4,0x5:8,0x16:4,0x11:1}.get(sub,0)
        need=cnt*bpi
        if i+need>n: break
        i+=need
    elif dtype in (0x11,):
        i+=5
    elif dtype in (0x55,0x66):
        i+=3
    elif dtype in (0x88,0x99):
        i+=2
    elif dtype==0xBB:
        i+=5
    else:
        i+=1

print('total scalar numerics',len(rows))
# remove obvious channel arrays paths? scalars only, so print grouped by top prefix
for t,p,v in rows[:2000]:
    if 'DataChannels' in p or 'RealTimeCapture' in p:
        continue
    print(f'{t:4} {p} = {v}')
