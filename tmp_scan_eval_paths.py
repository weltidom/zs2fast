import gzip

raw = gzip.open('/Users/dominicwelti/Documents/Single_lap_shear.zs2', 'rb').read()

i = 4
stack = []

while i < len(raw):
    if raw[i] == 0xFF:
        if stack:
            stack.pop()
        i += 1
        continue

    ln = raw[i]
    i += 1
    if i + ln > len(raw):
        break
    name = raw[i:i+ln].decode('utf-8', errors='replace')
    i += ln
    if i >= len(raw):
        break

    dtype = raw[i]
    path = '/'.join(stack + [name])

    if 'EvalContext' in path or 'ParamContext' in path:
        if dtype == 0xEE:
            j = i + 1
            sub = int.from_bytes(raw[j:j+2], 'little')
            j += 2
            cnt = int.from_bytes(raw[j:j+4], 'little')
            print(f'EE sub=0x{sub:04X} cnt={cnt:4d} | {path}')
        elif dtype in (0xAA, 0x00):
            print(f'STR              | {path}')
        elif dtype == 0xDD:
            print(f'NEST             | {path}')

    if dtype == 0xDD:
        ln2 = raw[i+1] if i + 1 < len(raw) else 0
        i += 2 + ln2
        stack.append(name)
    elif dtype == 0xEE:
        i += 1
        sub = int.from_bytes(raw[i:i+2], 'little')
        i += 2
        cnt = int.from_bytes(raw[i:i+4], 'little')
        i += 4
        b = {0x0004:4, 0x0005:8, 0x0016:4, 0x0011:1, 0x0000:0}.get(sub, 0)
        i += cnt * b
    elif dtype in (0xAA, 0x00):
        i += 1
        chars = int.from_bytes(raw[i:i+4], 'little') & 0x7FFFFFFF
        i += 4 + chars * 2
    elif dtype in (0x11, 0x22, 0x33, 0x44, 0xBB):
        i += 5
    elif dtype in (0x55, 0x66):
        i += 3
    elif dtype in (0x88, 0x99):
        i += 2
    elif dtype == 0xCC:
        i += 9
    elif dtype == 0xFF:
        i += 1
    else:
        break
