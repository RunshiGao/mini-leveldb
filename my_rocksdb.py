import os
import csv
import datetime
import glob
from lsm_tree import LSMTree
import util

BLOCK_SIZE = 256  # bytes
INITIAL_SIZE = 1024 * 1024  # 1 MByte
METADATA_SIZE = 1  # block


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
                  # first 9 blocks is for meta data and FCB, 9-45 bitmap
                  # each 4 blocks is for one db_file, max 9 db_file
                  f.write(('f'*11 + '8').ljust(9216, '0')) 



def put(myfile: str):
      with open(db_file, "r+") as f1:
            fcb_block = 0
            # check if myfile exists
            for i in range(1,9):
                  data = util.read_block(db_file, i)
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

            free_block = util.get_free_block_and_set(db_file)
            f1.write(str(free_block).rjust(5,'0'))
            f1.seek(free_block * BLOCK_SIZE)
            block_used = 1
            with open(myfile, "r", encoding="latin-1") as f2:
                  reader = csv.reader(f2, delimiter="\t")
                  next(reader, None)
                  cnt = 0
                  for line in reader:
                        line = ''.join(str(e) for e in line)
                        line = util.remove_two_byte_characters(line)
                        f1.write(line)
                        cnt += 1
                        if cnt == 6:
                              # a block is full, update bitmap
                              cnt = 0
                              f1.write(''.ljust(BLOCK_SIZE - 240 - 5))
                              free_block = util.get_free_block_and_set(db_file)
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
            
      create_index_for_file(myfile)

def create_index_for_file(myfile: str):
      height = util.calculate_lsmt_height(myfile)
      global lsmt_index
      lsmt_index = LSMTree(height, 0.5, dbfile=db_file, myfile=myfile)
      # with open(db_file, "r+") as f:
      # check if myfile exists
      fcb_block_num = util.get_fcb_block_num(db_file, myfile)
      # get the free block for index
      index_block = util.get_free_block_and_set(db_file)
      wal_block = util.get_free_block_and_set(db_file)
      util.write_block(db_file, index_block, f"{wal_block}0".rjust(256))
      fcb_block_data = util.read_block(db_file, fcb_block_num)
      util.write_block(db_file, fcb_block_num, (fcb_block_data[:95] + str(index_block).rjust(5,'0')).ljust(BLOCK_SIZE))
      starting_block = fcb_block_data[80:85]
      cur_block = starting_block
      while cur_block != "99999":
            cur_block_data = util.read_block(db_file, int(cur_block))
            rows = [cur_block_data[i*40:(i+1)*40] for i in range(6)]
            for row in rows:
                  if row[0] == ' ': continue
                  key, value = row.split(',',1)
                  lsmt_index.put(key, cur_block)
            next_block = cur_block_data[-5:]
            cur_block = next_block
            
      # lsmt_index.print()

def dir():
      for i in range(1, 9):
            fcb = util.read_block(db_file, i)
            if(fcb[0] == ' '): continue
            filename = fcb[:50].strip()
            size = fcb[50:60].strip()
            date = fcb[60:80].strip()
            print(f"{filename}     {size} bytes      {date}")

def get(myfile: str):
      # check if myfile exists
      fcb_block_num = util.get_fcb_block_num(db_file, myfile)
      fcb_block_data = util.read_block(db_file, fcb_block_num)
      starting_block = fcb_block_data[80:85]
      cur_block = starting_block
      with open("get-"+myfile, "w") as f2:
            writer = csv.writer(f2)
            while cur_block != "99999":
                  cur_block_data = util.read_block(db_file, int(cur_block))
                  util.write_data_block_to_csv(writer, cur_block_data)
                  next_block = cur_block_data[-5:]
                  cur_block = next_block

def find(myfile: str, key: int):
      data_block, reads = lsmt_index.get(key)
      if data_block == "99999":
            print(f"key {key} not found, # of blocks = {str(reads)}")
            return
      content = util.read_block(db_file, int(data_block))
      for i in range(0, util.BLOCK_SIZE-40, 40):
            entry = content[i:i+40]
            k, v = entry.split(',', 1)
            if int(k) == key:
                  print(f"found entry: {entry}, # of blocks = {str(reads)}")
                  return

def rm(myfile: str):
      # check if myfile exists
      fcb_block_num = util.get_fcb_block_num(db_file, myfile)
      if fcb_block_num == -1:
            print("File not found")
            return
      fcb_block_data = util.read_block(db_file, fcb_block_num)
      starting_block = fcb_block_data[80:85]
      # print(starting_block, ending_block)
      cur_block = starting_block
      while cur_block != "99999":
            cur_block_data = util.read_block(db_file, int(cur_block))
            next_block = cur_block_data[-5:]
            util.mark_block_free(db_file, int(cur_block))
            # write_block(f, int(cur_block), ' ' * BLOCK_SIZE)
            cur_block = next_block
      util.write_block(db_file, fcb_block_num, ' ' * BLOCK_SIZE)
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
      # dir()
      find("movies-small.csv", 47950)
      find("movies-small.csv", 193609)
      find("movies-small.csv", 1)
      find("movies-small.csv", 0)
      find("movies-small.csv", 2894)
      find("movies-small.csv", 2895)
      # kill("test_group1")


if __name__ == "__main__":
    main()
