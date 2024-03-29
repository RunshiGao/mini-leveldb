# encoding = 'utf-8'

import random

# Node structure of SkipList
class Node:
    # key is the key it stores
    # level is the level it will be created at
    def __init__(self, key, level):
        self.key = key
        self.level = level
        # forward is the pointer pointing to the next nodes
        self.forward = [None] * (level + 1)


class SkipList:
    # max_lvl is the max level we can have for the skip list
    # P is the probability a node to promote to the next level
    def __init__(self, max_lvl, P):
        self.MAXLVL = max_lvl
        self.P = P
        # dummy node is created at levle -1
        self.header = self.createNode(self.MAXLVL, -1)
        self.level = 0

    def createNode(self, lvl, key):
        return Node(key, lvl)

    # helper function to get a random level below the max_lvl
    def randomLevel(self):
        lvl = 0
        while random.random() < self.P and lvl < self.MAXLVL:
            lvl += 1
        return lvl

    # Insert the key into the storage
    def insert(self, key):
        # assume integer key
        key = int(key)
        # the list storing the node we need to update, in other word, the path we went
        update = [None] * (self.MAXLVL + 1)
        # create a pointer to traverse the skip list
        current = self.header
        # traverse the skip list from the highest level
        for i in range(self.level, -1, -1):
            # find a node that is greater than the current node
            while current.forward[i] and current.forward[i].key < key:
                # move forward until finding the right position for insertion
                current = current.forward[i]
            # after found a greater one, store the current node into path
            update[i] = current

        # Get to the position to store
        current = current.forward[0]

        # insert logic
        if current is None or current.key != key:
            # get a random level
            rlevel = self.randomLevel()
            # update the max level
            if rlevel > self.level:
                # new level will be pointing to dummy head
                for i in range(self.level + 1, rlevel + 1):
                    update[i] = self.header
                self.level = rlevel
            # create node at the new level
            n = self.createNode(rlevel, key)
            # update the last node visited on each level pointing to the new node
            for i in range(rlevel + 1):
                n.forward[i] = update[i].forward[i]
                update[i].forward[i] = n
        print("Sucessfully inserted key:" + str(key))
    # search the node containing the key, return True if found otherwise False
    def search(self, key):
        # basically the same logic as insertion, insertion is just first search to the right place then insert
        key = int(key)
        current = self.header
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
                print("level " + str(current.level) + ":" + str(current.key))
        current = current.forward[0]
        print("level " + str(current.level) + ":" + str(current.key))
        if current and current.key == key:
            return True
        return False

    # print the list at each level
    def displayList(self):
        print("Printing Skip List:")
        for lvl in range(self.level, -1, -1):
            print("Level {}: ".format(lvl), end=" ")
            node = self.header.forward[lvl]
            while node:
                print(node.key, end=" ")
                node = node.forward[lvl]
            print("")