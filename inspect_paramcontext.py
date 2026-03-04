import gzip
import struct

ROOT = 'Document/Body/batch/Series/EvalContext/ParamContext'

with gzip.open('/Users/dominicwelti/Documents/Single_lap_shear.zs2', 'rb') as f:
    data = f.read()

i = 4
n = len(data)
name_stack = []
rows = []

while i < n and len(rows) < 400:
    if data[i] == 0xFF:
        if name_stack:
            name_stack.pop()
        i += 1
        continue

    name_len = data[i]
    i += 1
    if i + name_len > n:
        break
    name = data[i:i+name_len].decode('ascii', errors='ignore')
    i += name_len
    if i >= n:
        break

    dtype = data[i]
    path = '/'.join(name_stack) + '/' + name

    if ROOT in path and 'EigenschaftenListe' not in path:
        rec = None
        if dtype in [0xAA, 0x00]:
            i += 1
            raw = int.from_bytes(data[i:i+4], 'little'); i += 4
            cnt = raw & 0x7FFFFFFF
            need = cnt * 2
            txt = data[i:i+need].decode('utf-16le', errors='ignore').rstrip('\x00') if i+need <= n else ''
            rec = (path, 'text', txt)
            i += need
        elif dtype == 0xEE:
            i += 1
            sub = int.from_bytes(data[i:i+2], 'little') if i+2<=n else 0
            cnt = int.from_bytes(data[i+2:i+6], 'little') if i+6<=n else 0
            i += 6
            bpi = {0x4:4,0x5:8,0x16:4,0x11:1}.get(sub,0)
            need = cnt*bpi
            preview = None
            if i+need<=n and cnt>0 and bpi>0:
                if sub == 0x4:
                    preview = struct.unpack('<f', data[i:i+4])[0]
                elif sub == 0x5:
                    preview = struct.unpack('<d', data[i:i+8])[0]
                elif sub == 0x16:
                    preview = struct.unpack('<I', data[i:i+4])[0]
                elif sub == 0x11:
                    preview = data[i]
            rec = (path, f'EE{sub:04X}[{cnt}]', preview)
            i += need
        elif dtype == 0xDD:
            i += 1
            dd_len = data[i] if i < n else 0
            i += 1 + dd_len
            name_stack.append(name)
            rec = (path, 'section', f'dd_len={dd_len}')
        elif dtype == 0x11:
            i += 1
            val = data[i] if i < n else None
            i += 4
            rec = (path, 'u8', val)
        elif dtype in [0x22,0x33,0x44]:
            i += 1
            raw4 = data[i:i+4]
            val = None
            if len(raw4)==4:
                if dtype==0x22: val = struct.unpack('<i', raw4)[0]
                elif dtype==0x33: val = struct.unpack('<I', raw4)[0]
                else: val = struct.unpack('<f', raw4)[0]
            i += 4
            rec = (path, {0x22:'i32',0x33:'u32',0x44:'f32'}[dtype], val)
        elif dtype in [0x55,0x66]:
            i += 3
            rec = (path, 'u16-ish', None)
        elif dtype in [0x88,0x99]:
            i += 2
            rec = (path, 'u8-ish', None)
        elif dtype == 0xBB:
            i += 5
            rec = (path, 'bb-u32', None)
        elif dtype == 0xCC:
            i += 1
            v = struct.unpack('<d', data[i:i+8])[0] if i+8<=n else None
            i += 8
            rec = (path, 'f64', v)
        else:
            i += 1

        if rec:
            rows.append(rec)
        continue

    # default skip when not under target root
    if dtype in [0xAA, 0x00]:
        i += 1
        if i + 4 > n: break
        raw = int.from_bytes(data[i:i+4], 'little'); i += 4
        need = (raw & 0x7FFFFFFF) * 2
        if i + need > n: break
        i += need
    elif dtype == 0xDD:
        i += 1
        if i >= n: break
        dd_len = data[i]
        i += 1 + dd_len
        name_stack.append(name)
    elif dtype == 0xEE:
        i += 1
        if i + 6 > n: break
        sub = int.from_bytes(data[i:i+2], 'little')
        cnt = int.from_bytes(data[i+2:i+6], 'little')
        i += 6
        bpi = {0x04:4,0x05:8,0x16:4,0x11:1}.get(sub,0)
        need = cnt*bpi
        if i + need > n: break
        i += need
    elif dtype in [0x11,0x22,0x33,0x44]:
        i += 5
    elif dtype in [0x55,0x66]:
        i += 3
    elif dtype in [0x88,0x99]:
        i += 2
    elif dtype == 0xBB:
        i += 5
    elif dtype == 0xCC:
        i += 9
    else:
        i += 1

for p,t,v in rows:
    print(f'{t:14} {p}\n  -> {v}')
