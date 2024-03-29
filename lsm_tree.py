from SkipList import SkipList
class LSMTree:
    # constructor to initialize the LSMTree
    def __init__(self, max_lvl=None, P=None):
        if max_lvl != None and P != None:
            self.max_lvl = max_lvl
            self.P = P
        else:
            self.max_lvl = 3
            self.P = 0.5

        self.memtable = SkipList(self.max_lvl, self.P)

    # put the key into the db
    def put(self, key) -> None:
        self.memtable.insert(key)

    # get the key from the db, return True if found
    def get(self, key) -> bool:
        return self.memtable.search(key)
    
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

