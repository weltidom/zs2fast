import gzip
import struct

def decode_utf16le(data):
    """Decode UTF-16LE bytes."""
    try:
        return data.decode('utf-16le').rstrip('\x00')
    except:
        return ""

def parse_zs2_params(filename):
    with gzip.open(filename, 'rb') as f:
        data = f.read()
    
    # Skip marker
    i = 4
    n = len(data)
    name_stack = []
    
    elem_info = {}  # elem_idx -> {name, unit, values_dict}
    
    # Track some sample paths
    sample_paths = []
    
    def extract_elem_idx(path):
        if '/Elem' in path:
            start = path.find('/Elem')
            rest = path[start+5:]
            if '/' in rest:
                end = rest.find('/')
                try:
                    return int(rest[:end])
                except:
                    pass
            else:
                try:
                    return int(rest)
                except:
                    pass
        return None
    
    while i < n:
        if data[i] == 0xFF:
            if name_stack:
                name_stack.pop()
            i += 1
            continue
        
        # Read name
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
        
        # Capture sample paths
        if '/EigenschaftenListe/' in path and len(sample_paths) < 20:
            sample_paths.append(path)
        
        # Track elem index
        current_elem = None
        if '/EigenschaftenListe/' in path:
            current_elem = extract_elem_idx(path)
            if current_elem is not None and current_elem not in elem_info:
                elem_info[current_elem] = {'name': '', 'unit': '', 'values': {}, 'path': path}
        
        # Extract strings (0xAA or 0x00)
        if dtype in [0xAA, 0x00]:
            i += 1
            if i + 4 > n:
                break
            raw = struct.unpack('<I', data[i:i+4])[0]
            i += 4
            char_count = raw & 0x7FFFFFFF
            needed = char_count * 2
            if i + needed > n:
                break
            text = decode_utf16le(data[i:i+needed])
            i += needed
            
            if '/EigenschaftenListe/' in path:
                elem_idx = extract_elem_idx(path)
                if elem_idx is not None:
                    if elem_idx not in elem_info:
                        elem_info[elem_idx] = {'name': '', 'unit': '', 'values': {}, 'path': path}
                    
                    if '/Name/Text' in path:
                        elem_info[elem_idx]['name'] = text
                    elif path.endswith('/EinheitName'):
                        elem_info[elem_idx]['unit'] = text
                    else:
                        # Store other text values
                        elem_info[elem_idx]['values'][name] = text
        
        # Extract scalars
        elif dtype == 0x22:  # i32
            i += 1
            if i + 4 > n:
                break
            val = struct.unpack('<i', data[i:i+4])[0]
            i += 4
            if '/EigenschaftenListe/' in path:
                elem_idx = extract_elem_idx(path)
                if elem_idx is not None:
                    if elem_idx not in elem_info:
                        elem_info[elem_idx] = {'name': '', 'unit': '', 'values': {}, 'path': path}
                    elem_info[elem_idx]['values'][name] = val
        
        elif dtype == 0x44:  # f32
            i += 1
            if i + 4 > n:
                break
            val = struct.unpack('<f', data[i:i+4])[0]
            i += 4
            if '/EigenschaftenListe/' in path:
                elem_idx = extract_elem_idx(path)
                if elem_idx is not None:
                    if elem_idx not in elem_info:
                        elem_info[elem_idx] = {'name': '', 'unit': '', 'values': {}, 'path': path}
                    elem_info[elem_idx]['values'][name] = val
        
        elif dtype == 0xCC:  # f64
            i += 1
            if i + 8 > n:
                break
            val = struct.unpack('<d', data[i:i+8])[0]
            i += 8
            if '/EigenschaftenListe/' in path:
                elem_idx = extract_elem_idx(path)
                if elem_idx is not None:
                    if elem_idx not in elem_info:
                        elem_info[elem_idx] = {'name': '', 'unit': '', 'values': {}, 'path': path}
                    elem_info[elem_idx]['values'][name] = val
        
        elif dtype == 0xDD:
            i += 2
            if i > n:
                break
            name_stack.append(name)
        
        elif dtype == 0xEE:
            i += 1
            if i + 6 > n:
                break
            sub = struct.unpack('<H', data[i:i+2])[0]
            cnt = struct.unpack('<I', data[i+2:i+6])[0]
            i += 6
            bytes_per = {0x04: 4, 0x05: 8, 0x16: 4, 0x11: 1}.get(sub, 0)
            needed = cnt * bytes_per
            if i + needed > n:
                break
            i += needed
        
        elif dtype in [0x11, 0x33]:
            i += 5
        elif dtype in [0x55, 0x66]:
            i += 3
        elif dtype in [0x88, 0x99]:
            i += 2
        elif dtype == 0xBB:
            i += 5
        else:
            i += 1
    
    return elem_info, sample_paths

# Test
info, sample_paths = parse_zs2_params('/Users/dominicwelti/Documents/Single_lap_shear.zs2')

print("=== Sample EigenschaftenListe paths ===")
for p in sample_paths:
    print(p)

print(f"\n\nFound {len(info)} parameter elements\n")

# Show first 30 with their values
for elem_idx in sorted(info.keys())[:30]:
    data = info[elem_idx]
    print(f"Elem {elem_idx}: {data['name']}")
    print(f"  Unit: {data['unit']}")
    if data['values']:
        print(f"  Values: {data['values']}")
    print()

# Show some elements with non-empty units
print("\n\n=== Elements with units and values ===")
for elem_idx in sorted(info.keys())[295:310]:
    data = info[elem_idx]
    if data.get('unit'):
        print(f"Elem {elem_idx}: {data['name']} [{data['unit']}]")
        if data['values']:
            print(f"  Values: {data['values']}")
        print()
