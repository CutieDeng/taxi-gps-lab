import pandas as pd 
import sys 

assert len(sys.argv) >= 5 

# Config of separate work 

TOTAL = int(sys.argv[2]) # split data as TOTAL parts 
IAM_START = int(sys.argv[3]) # start from IAM_START 
IAM_LENGTH = int(sys.argv[4]) # length of IAM_START + IAM_LENGTH - 
IAM_END = IAM_START + IAM_LENGTH 

print (f'TOTAL: {TOTAL}, IAM_START: {IAM_START}, IAM_LENGTH: {IAM_LENGTH}, IAM_END: {IAM_END}, output file: {sys.argv[1]}')

data = pd.read_csv('res/sample_taxi_region.csv')
block_data = pd.read_json('res/sz_block_mars.json')

block_data = block_data['features']

def in_this_region(coordiate, region) : 
    cross_cnt = 0 
    for i in range(len(region)) : 
        l, r = region[i-1], region[i] 
        if l[1] == r[1] : 
            continue 
        if (l[1] > coordiate[1]) == (r[1] > coordiate[1]): 
            continue 
        x = (coordiate[1] - l[1]) * (r[0] - l[0]) / (r[1] - l[1]) + l[0] 
        if x < coordiate[0]: 
            continue 
        lx = l[0] if l[0] < r[0] else r[0] 
        rx = l[0] if l[0] > r[0] else r[0] 
        if lx <= x <= rx : 
            pass 
        else: 
            continue 
        cross_cnt += 1 
    return cross_cnt % 2 == 1 

def in_which_block(coordinate) : 
    results = [] 
    for i in range(len(block_data)) : 
        regions = block_data[i]['geometry']['coordinates'] 
        for region in regions: 
            is_in_this_region = in_this_region(coordinate, region) 
            if is_in_this_region : 
                results.append((block_data[i]['properties']['NAME'], block_data[i]['properties']['DISTRICT'], block_data[i]['properties']['BLOCK_ID']))
                break 
    return results 
    

# split data average as TOTAL parts 
data_len = len(data) 
split_len = data_len // TOTAL 
if data_len % TOTAL != 0 : 
    split_len += 1 
my_data = data.iloc[split_len * IAM_START : split_len * IAM_END] 

# process bar 
import tqdm 
value = tqdm.trange(1 + (len(my_data) // 100))

# split data by block 
block_id = [-1 for _ in range(len(my_data))]
lon = my_data['lon'] 
lat = my_data['lat'] 
index = 0
for loni, lati in zip(lon, lat) : 
    if index % 100 == 0 : 
        value.update()
    i = in_which_block([loni, lati]) 
    if len(i) == 1: 
        block_id[index] = i[0][2] 
    index += 1 
value.update()

my_data['block_id'] = block_id 
my_data.to_csv(sys.argv[1], index=False) 