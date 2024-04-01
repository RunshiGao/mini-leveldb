from SkipList import SkipList
import util
import math
import bisect
class LSMTree:
    # constructor to initialize the LSMTree
    def __init__(self, max_lvl=None, P=None, capacity=1024, dbfile=None, myfile=None):
        if max_lvl != None and P != None:
            self.max_lvl = max_lvl
            self.P = P
        else:
            self.max_lvl = 3
            self.P = 0.5

        self.memtable = SkipList(self.max_lvl, self.P)
        self.capacity = capacity
        self.size = 0
        self.dbfile = dbfile
        self.myfile = myfile
    # put the key into the db
    def put(self, key, value) -> None:
        self.memtable.insert(key, value)
        self.size += 1
        if self.size == self.capacity:
            print("memtable full, flushing to disk")
            self.flush()
            self.size = 0
            self.memtable = SkipList(self.max_lvl, self.P)
        
    def flush(self) -> None:
        fcb_block = util.get_fcb_block_num(self.dbfile, self.myfile)
        index_block = int(util.read_block(self.dbfile, fcb_block)[95:100])
        index_block_data = util.read_block(self.dbfile, index_block)
        # print("index block data: " + index_block_data)
        num_of_sstable = int(index_block_data[-1])
        sstable_meta_block = str(util.get_free_block_and_set(self.dbfile)).rjust(5,'0')
        new_index_block_data = str(sstable_meta_block) + index_block_data[:num_of_sstable * 5] + index_block_data[(num_of_sstable + 1)* 5:-1] + str(num_of_sstable + 1)
        # print("new: "+new_index_block_data)
        util.write_block(self.dbfile, index_block, new_index_block_data)
        # write the metadata for SSTable at sstable_starting_block
        # first get the kv pairs from memtable
        all_nodes = self.memtable.get_all_nodes()
        next_free_block = str(util.get_free_block_and_set(self.dbfile)).rjust(5,'0')
        sstable_starting_block = next_free_block
        updated_sstable_meta_data = sstable_starting_block
        
        # convert to sstable after we get all nodes, 1 block can fit in 256/13 = 19 kv pairs
        sparse_index = math.ceil(1024 / 19 / 19)
        cnt = sparse_index
        for i in range(19, len(all_nodes), 19):
            nodes = [''.join(e) for e in all_nodes[i-19:i]]
            tmp = next_free_block
            next_free_block = str(util.get_free_block_and_set(self.dbfile)).rjust(5,'0')
            data = ''.join(nodes).ljust(251) + next_free_block
            util.write_block(self.dbfile, int(tmp), data)
            if cnt == sparse_index:
                key, _ = all_nodes[i-19]
                updated_sstable_meta_data += key + tmp
                cnt = 0
            cnt += 1
        # manage last block
        remainder = 1024 % 19
        nodes = [''.join(e) for e in all_nodes[-remainder:]]
        data = ''.join(nodes).ljust(251) + "99999"
        util.write_block(self.dbfile, int(next_free_block), data)
        key, _ = all_nodes[-remainder]
        updated_sstable_meta_data += key + next_free_block
        updated_sstable_meta_data = updated_sstable_meta_data.ljust(256)
        # print(f"updated ss meta of len({len(updated_sstable_meta_data)}): {updated_sstable_meta_data}" )
        util.write_block(self.dbfile, int(sstable_meta_block), updated_sstable_meta_data)


    # get the key from the db, return the data_block associated with the key, 99999 if not found
    def get(self, key):
        result = self.memtable.search(key)
        reads = 0
        # in found in memtable, just return
        if result != "99999":
            # print("found the key in memtable, # of blocks = 0")
            return result, reads
        # not found, iterate through sstable
        else:
            with open(self.dbfile, "r+") as f:
                fcb_block = util.get_fcb_block_num(self.dbfile, self.myfile)
                index_block = util.read_block(self.dbfile, int(fcb_block))[95:100]
                index_data = util.read_block(self.dbfile, int(index_block))
                reads = 3
                for i in range(0, 50, 5):
                    sstable_meta_block = index_data[i:i+5]
                    if sstable_meta_block[0] == ' ':
                        break
                    sstable_meta_data = util.read_block(self.dbfile, int(sstable_meta_block))
                    reads += 1
                    for i in range(5, util.BLOCK_SIZE - 26, 13):
                        k1 = int(sstable_meta_data[i:i+8])
                        sstable1 = sstable_meta_data[i+8:i+13]
                        k2 = int(sstable_meta_data[i+13:i+21])
                        sstable2 = sstable_meta_data[i+21:i+26]
                        # if key not in range, skip this sstable
                        if not k1 <= key <= k2:
                            continue
                        
                        pairs = []
                        # if in the range, scan
                        while sstable1 != sstable2:
                            sstable_cur_block_data = util.read_block(self.dbfile, int(sstable1))
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
                        break
                return "99999", reads



    
    # display the index structure
    def print(self):
        self.memtable.displayList()

    # Function to read a text file and insert keys into the memtable
    def insert_keys_from_file(self, filename):
        # open the file
        with open(filename, 'r') as file:
            # read a line
            for line in file:
                # Assuming keys are separated by comma
                keys = line.strip().split(',')  
                # insert each key
                for key in keys:
                    self.put(key)

