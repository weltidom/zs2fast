import gzip

with gzip.open('/Users/dominicwelti/Documents/Single_lap_shear.zs2', 'rb') as f:
    data = f.read()

i = 4
n = len(data)
name_stack = []
results = []

while i < n and len(results) < 200:
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

    if dtype in [0xAA, 0x00]:
        i += 1
        if i + 4 > n:
            break
        raw = int.from_bytes(data[i:i+4], 'little')
        i += 4
        char_count = raw & 0x7FFFFFFF
        needed = char_count * 2
        if i + needed > n:
            break
        text = data[i:i+needed].decode('utf-16le', errors='ignore').rstrip('\x00')
        i += needed

        lpath = path.lower()
        if 'eigenschaftenliste' in lpath and ('wert' in lpath or 'value' in lpath or 'result' in lpath or 'ergebnis' in lpath):
            results.append((path, text))

    elif dtype == 0xDD:
        i += 1
        if i >= n:
            break
        dd_len = data[i]
        i += 1 + dd_len
        name_stack.append(name)
    elif dtype == 0xEE:
        i += 1
        if i + 6 > n:
            break
        sub = int.from_bytes(data[i:i+2], 'little')
        cnt = int.from_bytes(data[i+2:i+6], 'little')
        i += 6
        bytes_per = {0x04: 4, 0x05: 8, 0x16: 4, 0x11: 1}.get(sub, 0)
        needed = cnt * bytes_per
        if i + needed > n:
            break
        i += needed
    elif dtype in [0x11, 0x22, 0x33, 0x44]:
        i += 5
    elif dtype in [0x55, 0x66]:
        i += 3
    elif dtype in [0x88, 0x99]:
        i += 2
    elif dtype == 0xBB:
        i += 5
    elif dtype == 0xCC:
        i += 9
    else:
        i += 1

print(f"Found {len(results)} matching text fields")
for p, t in results[:80]:
    print(p)
    print(f"  -> {t}")
