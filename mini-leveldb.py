import os
import csv
import datetime

BLOCK_SIZE = 256  # bytes
INITIAL_SIZE = 1024 * 1024  # 1 MByte
METADATA_SIZE = 1  # block


def get_free_block_and_set():
      with open(db_file, "r+") as f:
            for x in range(9,13):
                  data = read_block(f, x)
                  bitmap_binary = ''.join(format(int(c, 16), '04b') for c in data)
                  for i, bit in enumerate(bitmap_binary):
                        if bit == '0':
                              updated_bitmap_binary = bitmap_binary[:i] + '1' + bitmap_binary[i + 1:]
                              updated_bitmap_hex = hex(int(updated_bitmap_binary, 2))[2:].zfill(BLOCK_SIZE)
                              write_block(f, x, updated_bitmap_hex)
                              return i

def read_block(f,  block_num):
      f.seek(BLOCK_SIZE * block_num)
      data = f.read(BLOCK_SIZE)
      # print(f'read block {block_num}: {data}')
      return data

def write_block(f, block_num, block_content):
      f.seek(BLOCK_SIZE * block_num)
      f.write(block_content)


def open_db(db_name: str):
      global db_file
      db_file = db_name
      
      if os.path.exists(db_file):
            return
      else:
            # if file doesn't exist
            with open(db_name, "w") as f:
                  f.write(' ' * INITIAL_SIZE)
                  # Initialize metadata
                  f.seek(0)
                  f.write(db_name.ljust(50)) # 0-49 db name
                  f.write('0'.ljust(10)) # 50 - 59 total size
                  f.write('1'.ljust(10)) # 60 - 69 num of PFS file
                  f.write(str(BLOCK_SIZE).ljust(10)) # 70 - 79 block size
                  f.write('0') # 80 - 80 
                  # Initialize bitmap for free block management
                  f.seek(BLOCK_SIZE * 9)
                  f.write('FFF8' + '0' * 1020) # first 9 blocks is for meta data and FCB

def put(myfile: str):
      with open(db_file, "r+") as f1:
            # check if myfile exists
            for i in range(1,9):
                  data = read_block(f1, i)
                  if data[0] != ' ':
                        filename = data[:50].strip()
                        if filename == myfile:
                              return
                  else:
                        # insert new FCB
                        f1.seek(i * BLOCK_SIZE)
                        f1.write(myfile.ljust(50))
                        f1.write(str(os.path.getsize(myfile)).ljust(10))
                        f1.write(str(datetime.datetime.now())[:-7])
                        print(str(datetime.datetime.now())[:-7])
                        break

            free_block = get_free_block_and_set()
            f1.seek(free_block * BLOCK_SIZE)
            with open(myfile, "r", encoding="latin-1") as f2:
                  reader = csv.reader(f2, delimiter="\t")
                  next(reader, None)
                  cnt = 0
                  for line in reader:
                        line = ''.join(str(e) for e in line).ljust(40)[:40]
                        # print(len(line))
                        f1.write(line)
                        cnt += 1
                        if cnt == 6:
                              # a block is full, update bitmap
                              cnt = 0
                              f1.write(''.ljust(BLOCK_SIZE - 240 - 5))
                              f1.write('99999')
                              free_block = get_free_block_and_set()

def run():
     while True:
            command = input("NoSQL> ").strip().split(' ', 1)
            action = command[0]
            if action == 'open':
                  db_name = command[1] + '.db0'
            
def main():
      open_db('test_group1.db0')
      put("movies-small.csv")
      
      
      



if __name__ == "__main__":
    main()
