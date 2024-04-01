import math
import csv

BLOCK_SIZE = 256  # bytes
INITIAL_SIZE = 1024 * 1024  # 1 MByte
METADATA_SIZE = 1  # block

def get_free_block_and_set(db_file):
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
            else:
                  break
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