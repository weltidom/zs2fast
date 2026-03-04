import gzip
import struct

TARGET = '/EigenschaftenListe/Elem295/'

with gzip.open('/Users/dominicwelti/Documents/Single_lap_shear.zs2', 'rb') as f:
    data = f.read()

i = 4
n = len(data)
name_stack = []
rows = []

while i < n:
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

    def add(kind, value=''):
        if TARGET in path:
            rows.append((path, kind, value))

    if dtype in [0xAA, 0x00]:
        i += 1
        raw = int.from_bytes(data[i:i+4], 'little')
        i += 4
        char_count = raw & 0x7FFFFFFF
        need = char_count * 2
        txt = data[i:i+need].decode('utf-16le', errors='ignore').rstrip('\x00') if i+need <= n else ''
        add('text', txt)
        i += need
    elif dtype == 0x11:
        i += 1
        v = data[i] if i < n else None
        add('u8', v)
        i += 4
    elif dtype == 0x22:
        i += 1
        v = struct.unpack('<i', data[i:i+4])[0] if i+4 <= n else None
        add('i32', v)
        i += 4
    elif dtype == 0x33:
        i += 1
        v = struct.unpack('<I', data[i:i+4])[0] if i+4 <= n else None
        add('u32', v)
        i += 4
    elif dtype == 0x44:
        i += 1
        v = struct.unpack('<f', data[i:i+4])[0] if i+4 <= n else None
        add('f32', v)
        i += 4
    elif dtype in [0x55, 0x66]:
        i += 1
        v = int.from_bytes(data[i:i+2], 'little') if i+2 <= n else None
        add('u16-ish', v)
        i += 2
    elif dtype in [0x88, 0x99]:
        i += 1
        v = data[i] if i < n else None
        add('u8-ish', v)
        i += 1
    elif dtype == 0xBB:
        i += 1
        v = struct.unpack('<I', data[i:i+4])[0] if i+4 <= n else None
        add('bb-u32', v)
        i += 4
    elif dtype == 0xCC:
        i += 1
        v = struct.unpack('<d', data[i:i+8])[0] if i+8 <= n else None
        add('f64', v)
        i += 8
    elif dtype == 0xDD:
        i += 1
        dd_len = data[i] if i < n else 0
        i += 1 + dd_len
        name_stack.append(name)
        add('section', f'dd_len={dd_len}')
    elif dtype == 0xEE:
        i += 1
        sub = int.from_bytes(data[i:i+2], 'little') if i+2 <= n else 0
        cnt = int.from_bytes(data[i+2:i+6], 'little') if i+6 <= n else 0
        i += 6
        bpi = {0x4:4,0x5:8,0x16:4,0x11:1}.get(sub,0)
        need = cnt*bpi
        preview = ''
        if cnt > 0 and bpi > 0 and i+need <= n:
            if sub == 0x4 and cnt >= 1:
                preview = struct.unpack('<f', data[i:i+4])[0]
            elif sub == 0x5 and cnt >= 1:
                preview = struct.unpack('<d', data[i:i+8])[0]
            elif sub == 0x16 and cnt >= 1:
                preview = struct.unpack('<I', data[i:i+4])[0]
        add(f'EE{sub:04X}[{cnt}]', preview)
        i += need
    else:
        add(f'unknown 0x{dtype:02X}', '')
        i += 1

print(f'Rows for Elem295: {len(rows)}')
for p,k,v in rows[:300]:
    print(f'{k:12} {p}\n  -> {v}')
