from SkipList import SkipList
import util
import math
import bisect
class LSMTree:
    # constructor to initialize the LSMTree
    def __init__(self, max_lvl=None, P=None, capacity=1024, dbfile=None, myfile=None, wal_block=None):
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
        self.wal_block = wal_block

    # put the key into the db
    def put(self, key, value) -> None:
        self.memtable.insert(key, value)
        self.size += 1
        if self.size == self.capacity:
            print("memtable full, flushing to disk")
            self.flush()
            self.size = 0
            self.memtable = SkipList(self.max_lvl, self.P)
    
    # def create_wal(self):
    #     wal_block = str(util.get_free_block_and_set(self.dbfile)).rjust(5, '0')
    #     wal_starting_block = str(util.get_free_block_and_set(self.dbfile)).rjust(5, '0')
    #     util.write_block(self.dbfile, int(wal_block), wal_starting_block + '0000'.rjust(251))

    # def delete_wal(self):
    #     return

    def flush(self) -> None:
        # first read index info
        fcb_block = util.get_fcb_block_num(self.dbfile, self.myfile)
        if fcb_block < 0:
            return
        index_block = int(util.read_block(self.dbfile, fcb_block)[95:100])
        index_block_data = util.read_block(self.dbfile, index_block)
        # add a new sstable and update num of sstable
        num_of_sstable = int(index_block_data[-2:])
        sstable_meta_block = str(util.get_free_block_and_set(self.dbfile)).rjust(5,'0')
        new_index_block_data = str(sstable_meta_block) + index_block_data[:num_of_sstable * 5] + index_block_data[(num_of_sstable + 1)* 5:-2] + str(num_of_sstable + 1).rjust(2,'0')
        
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
        return result, reads

    # display the index structure
    def print(self):
        self.memtable.displayList()


