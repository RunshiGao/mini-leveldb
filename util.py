import math
import csv
import os
import bisect

BLOCK_SIZE = 256  # bytes
INITIAL_SIZE = 1024 * 1024  # 1 MByte
METADATA_SIZE = 1  # block

def check_need_extend(db_file):
      cnt = 0
      for x in range(9,45,4):
            cnt += 1
            data = read_block(db_file, x + 3)
            if data[-1] == '0':
                  break
            elif data[-1] == 'f':
                  if os.path.exists(db_file[:-1] + str(cnt)):
                        continue
                  else:
                        return True
      return False

def get_free_block_and_set(db_file):
      if check_need_extend(db_file):
            extend(db_file)
      for x in range(9,45):
            offset = (x - 9) * 1024
            data = read_block(db_file, x)
            bitmap_binary = ''.join(format(int(c, 16), '04b') for c in data)
            for i, bit in enumerate(bitmap_binary):
                  if bit == '0':
                        updated_bitmap_binary = bitmap_binary[:i] + '1' + bitmap_binary[i + 1:]
                        updated_bitmap_hex = hex(int(updated_bitmap_binary, 2))[2:].zfill(BLOCK_SIZE)
                        write_block(db_file, x, updated_bitmap_hex)
                        return i + offset

def extend(db_file):
      print("extending pfs file")
      # first modify the metadata in db0
      meta_data = read_block(db_file, 0)
      total_size = str(int(meta_data[50:60].strip()) + INITIAL_SIZE).ljust(10)
      num_pfs = int(meta_data[60:70].strip()) + 1
      updated = meta_data[:50] + total_size + str(num_pfs).ljust(10) + meta_data[70:]
      write_block(db_file, 0, updated)
      with open(db_file[:-1] + str(num_pfs - 1), 'w') as f:
            f.write(' ' * INITIAL_SIZE)

def get_from_sstable(dbfile, myfile, key):
      fcb_block = get_fcb_block_num(dbfile, myfile)
      if fcb_block == -1:
            return "99999", 0
      index_block = read_block(dbfile, int(fcb_block))[95:100]
      index_data = read_block(dbfile, int(index_block))
      reads = 3
      for i in range(0, 250, 5):
            sstable_meta_block = index_data[i:i+5]
            if sstable_meta_block[0] == ' ':
                  break
            sstable_meta_data = read_block(dbfile, int(sstable_meta_block))
            reads += 1
            for i in range(5, BLOCK_SIZE - 26, 13):
                  k1 = int(sstable_meta_data[i:i+8])
                  sstable1 = sstable_meta_data[i+8:i+13]
                  if(sstable_meta_data[i+13] == " "):
                        break
                  k2 = int(sstable_meta_data[i+13:i+21])
                  sstable2 = sstable_meta_data[i+21:i+26]
                  # if key not in range, skip this sstable
                  if not k1 <= key <= k2:
                        continue
                  
                  pairs = []
                  # if in the range, scan
                  while sstable1 != sstable2:
                        sstable_cur_block_data = read_block(dbfile, int(sstable1))
                        reads += 1
                        for i in range(0, 247, 13):
                              k, v = sstable_cur_block_data[i:i+8], sstable_cur_block_data[i+8:i+13]
                              pairs.append((int(k),v))
                        sstable1 = sstable_cur_block_data[-5:]
                  pos = bisect.bisect_left(pairs, key, key=lambda x: x[0])
                  # print(f"pos:{pos}, value: {pairs[pos]}")
                  # found key
                  if pairs[pos][0] == key:
                        return pairs[pos][1], reads
                  else:
                        return "99999", reads
      return "99999", reads

def mark_block_free(db_file, block_num):
      data = ''.join([read_block(db_file, i) for i in range(9, 13)])
      bitmap_binary = ''.join(format(int(c, 16), '04b') for c in data)
      updated_bitmap_binary = bitmap_binary[:block_num] + '0' + bitmap_binary[block_num + 1:]
      updated_bitmap_hex = hex(int(updated_bitmap_binary, 2))[2:].zfill(BLOCK_SIZE)
      write_block(db_file, 9, updated_bitmap_hex)

def read_block(db_file: str, block_num: int):
      extention_num = block_num // 4096
      with open(db_file[:-1] + str(extention_num), 'r') as f:
            f.seek(BLOCK_SIZE * (block_num % 4096))
            data = f.read(BLOCK_SIZE)
            # print(f'read block {block_num} of len({len(data)}): {data}')
            return data

def write_block(db_file: str, block_num: int, block_content:str):
      extention_num = block_num // 4096
      with open(db_file[:-1] + str(extention_num), 'r+') as f:
            f.seek(BLOCK_SIZE * (block_num % 4096))
            f.write(block_content)

def remove_two_byte_characters(input_string: str):
    return ''.join(char for char in input_string if len(char.encode('utf-8')) <= 1).ljust(40)[:40]

def write_data_block_to_csv(f, data: str):
      rows = [data[i*40:(i+1)*40] for i in range(6)]
      for row in rows:
            if row[0] == ' ': continue
            key, value = row.split(',',1)
            f.writerow([key, value])

def get_fcb_block_num(db_file, myfile):
      for i in range(1,9):
            data = read_block(db_file, i)
            if data[0] != ' ':
                  filename = data[:50].strip()
                  if filename == myfile:
                        return i
      return -1

def calculate_lsmt_height(myfile: str):
      rows = 0
      with open(myfile, "r", encoding="latin-1") as f:
            reader = csv.reader(f, delimiter="\t")
            next(reader, None)
            for line in reader:
                  rows += 1
      height = int(math.log2(rows))
      return height