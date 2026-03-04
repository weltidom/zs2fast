import gzip

with gzip.open('/Users/dominicwelti/Documents/Single_lap_shear.zs2', 'rb') as f:
    data = f.read()

i = 4
n = len(data)
name_stack = []

# Track field names seen in EigenschaftenListe/Elem structures
elem_fields = {}

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
    path = name_stack[:]
    path.append(name)
    full_path = '/'.join(path)
    
    # Look for EigenschaftenListe/Elem patterns
    if 'EigenschaftenListe' in full_path:
        # Find Elem index if any
        elem_idx = None
        for segment in path:
            if segment.startswith('Elem') and len(segment) > 4:
                try:
                    elem_idx = int(segment[4:])
                    break
                except:
                    pass
        
        if elem_idx is not None:
            if elem_idx not in elem_fields:
                elem_fields[elem_idx] = []
            # Store the field name within this Elem
            idx = path.index(f'Elem{elem_idx}')
            if idx + 1 < len(path):
                field_path = '/'.join(path[idx+1:])
                if field_path and field_path not in elem_fields[elem_idx]:
                    elem_fields[elem_idx].append(field_path)
    
    # Skip based on dtype
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
        i += needed
    elif dtype == 0xDD:
        i += 2
        if i > n:
            break
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

# Show fields for first few Elems
print(f"Found fields for {len(elem_fields)} Elem elements")
for elem_idx in sorted(elem_fields.keys())[:10]:
    print(f"\nElem{elem_idx} fields:")
    for field in elem_fields[elem_idx][:15]:
        print(f"  {field}")
