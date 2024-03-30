import os
import csv
import datetime
import glob

BLOCK_SIZE = 256  # bytes
INITIAL_SIZE = 1024 * 1024  # 1 MByte
METADATA_SIZE = 1  # block


def get_free_block_and_set():
      with open(db_file, "r+") as f:
            for x in range(9,13):
                  offset = (x - 9) * 1024
                  data = read_block(f, x)
                  bitmap_binary = ''.join(format(int(c, 16), '04b') for c in data)
                  for i, bit in enumerate(bitmap_binary):
                        if bit == '0':
                              updated_bitmap_binary = bitmap_binary[:i] + '1' + bitmap_binary[i + 1:]
                              updated_bitmap_hex = hex(int(updated_bitmap_binary, 2))[2:].zfill(BLOCK_SIZE)
                              write_block(f, x, updated_bitmap_hex)
                              return i + offset

def mark_block_free(block_num):
      with open(db_file, "r+") as f:
            data = ''.join([read_block(f, i) for i in range(9, 13)])
            bitmap_binary = ''.join(format(int(c, 16), '04b') for c in data)
            updated_bitmap_binary = bitmap_binary[:block_num] + '0' + bitmap_binary[block_num + 1:]
            updated_bitmap_hex = hex(int(updated_bitmap_binary, 2))[2:].zfill(BLOCK_SIZE)
            write_block(f, 9, updated_bitmap_hex)

def read_block(f,  block_num):
      f.seek(BLOCK_SIZE * block_num)
      data = f.read(BLOCK_SIZE)
      # print(f'read block {block_num} of len({len(data)}): {data}')
      return data

def write_block(f, block_num, block_content):
      f.seek(BLOCK_SIZE * block_num)
      f.write(block_content)

def remove_two_byte_characters(input_string: str):
    return ''.join(char for char in input_string if len(char.encode('utf-8')) <= 1).ljust(40)[:40]

def write_data_block_to_csv(f, data: str):
      rows = [data[i*40:(i+1)*40] for i in range(6)]
      for row in rows:
            if row[0] == ' ': continue
            key, value = row.split(',',1)
            f.writerow([key, value])

def get_fcb_block_num(f, myfile):
      for i in range(1,9):
            data = read_block(f, i)
            if data[0] != ' ':
                  filename = data[:50].strip()
                  if filename == myfile:
                        return i
      return -1

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
                  f.write('1048576'.ljust(10)) # 50 - 59 total size
                  f.write('1'.ljust(10)) # 60 - 69 num of PFS file
                  f.write(str(BLOCK_SIZE).ljust(10)) # 70 - 79 block size
                  f.write('0') # 80 - 80 
                  # Initialize bitmap for free block management
                  f.seek(BLOCK_SIZE * 9)
                  f.write('FFF8' + '0' * 1020) # first 9 blocks is for meta data and FCB



def put(myfile: str):
      with open(db_file, "r+") as f1:
            fcb_block = 0
            # check if myfile exists
            for i in range(1,9):
                  data = read_block(f1, i)
                  if data[0] != ' ':
                        filename = data[:50].strip()
                        if filename == myfile:
                              return
                  else:
                        # insert new FCB
                        fcb_block = i
                        f1.seek(i * BLOCK_SIZE)
                        f1.write(myfile.ljust(50))
                        f1.write(str(os.path.getsize(myfile)).ljust(10))
                        f1.write(str(datetime.datetime.now())[:-7].ljust(20))
                        break

            free_block = get_free_block_and_set()
            f1.write(str(free_block).rjust(5,'0'))
            f1.seek(free_block * BLOCK_SIZE)
            block_used = 1
            with open(myfile, "r", encoding="latin-1") as f2:
                  reader = csv.reader(f2, delimiter="\t")
                  next(reader, None)
                  cnt = 0
                  for line in reader:
                        line = ''.join(str(e) for e in line)
                        line = remove_two_byte_characters(line)
                        f1.write(line)
                        cnt += 1
                        if cnt == 6:
                              # a block is full, update bitmap
                              cnt = 0
                              f1.write(''.ljust(BLOCK_SIZE - 240 - 5))
                              free_block = get_free_block_and_set()
                              # print("got free block:", free_block)
                              f1.write(str(free_block).rjust(5,'0'))
                              block_used += 1
                  # manage last block
                  f1.write('99999'.rjust(256 - cnt * 40))
            # print("last free block:", free_block)
            # update metadata, # of KV tables
            f1.seek(80)
            f1.write('1')
            # update ending block and number of blocks used
            f1.seek(fcb_block*BLOCK_SIZE+85)
            f1.write(str(free_block).rjust(5,'0'))
            # print("block used:", block_used)
            f1.write(str(block_used).rjust(5,'0'))
            
def dir():
      with open(db_file, "r") as f:
            for i in range(1, 9):
                  fcb = read_block(f, i)
                  if(fcb[0] == ' '): continue
                  filename = fcb[:50].strip()
                  size = fcb[50:60].strip()
                  date = fcb[60:80].strip()
                  print(f"{filename}     {size} bytes      {date}")

def get(myfile: str):
      with open(db_file, "r") as f:
            # check if myfile exists
            fcb_block_num = get_fcb_block_num(f, myfile)
            fcb_block_data = read_block(f, fcb_block_num)
            starting_block = fcb_block_data[80:85]
            cur_block = starting_block
            with open("get-"+myfile, "w") as f2:
                  writer = csv.writer(f2)
                  while cur_block != "99999":
                        cur_block_data = read_block(f, int(cur_block))
                        write_data_block_to_csv(writer, cur_block_data)
                        next_block = cur_block_data[-5:]
                        cur_block = next_block

def rm(myfile: str):
      with open(db_file, "r+") as f:
            # check if myfile exists
            fcb_block_num = get_fcb_block_num(f, myfile)
            if fcb_block_num == -1:
                  print("File not found")
                  return
            fcb_block_data = read_block(f, fcb_block_num)
            starting_block = fcb_block_data[80:85]
            # print(starting_block, ending_block)
            cur_block = starting_block
            while cur_block != "99999":
                  cur_block_data = read_block(f, int(cur_block))
                  next_block = cur_block_data[-5:]
                  mark_block_free(int(cur_block))
                  write_block(f, int(cur_block), ' ' * BLOCK_SIZE)
                  cur_block = next_block
            write_block(f, fcb_block_num, ' ' * BLOCK_SIZE)
      print(myfile + "removed")

def kill(PFSfilename: str):
      files = glob.glob(PFSfilename + '.db*')
      for file in files:
            print("removing file:" + file)
            os.remove(file)

def run():
     while True:
            command = input("NoSQL> ").strip().split(' ', 1)
            action = command[0]
            if action == 'open':
                  db_name = command[1] + '.db0'
            
def main():
      open_db('test_group1.db0')
      put("movies-small.csv")
      dir()
      # get("movies-small.csv")
      # rm("movies-small.csv")
      dir()
      kill("test_group1")


if __name__ == "__main__":
    main()
