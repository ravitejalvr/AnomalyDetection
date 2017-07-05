"""
Created a class name user for handling the friends, purchases invidually for each user.

Then created functions Build Purchases Networks, Make friends, Unfriends, get Network Purchases for completing the problem given.

Input is taken from batch_log.json for training purchases network and stream_log.json for selecting the anamolous purchases which are saved in the file flagged_purchases.json

Python is chosen because of its simplicity and the implementation of the solution is made with enough documentation and modularity.

"""
from __future__ import print_function
from collections import defaultdict
import json
import sys
import statistics
import datetime as dt
from pprint import pprint

class User:
    
    def __init__(self, id):
        self.id = id
        self.friends = defaultdict()
        self.purchases = []

    def getid(self):
        return self.id
    
    def getFriends(self):
        return self.friends

    def printPurchases(self):
        print(self.purchases)

    def getPurchases(self):
        return self.purchases
    
    def addPurchase(self, timestamp, amount):
        self.purchases.append({'Timestamp': timestamp, 'Amount': amount})
    
    def removeFriend(self, oldFriend):        
        if(oldFriend in self.friends):
            self.friends.__delitem__(oldFriend)
        
    def addFriend(self, newFriend):
        if(not self.friends.__contains__(newFriend.getid())):
            self.friends[newFriend.getid()] = newFriend

    def isFriend(self, newFriendId):
        return self.friends.__contains__(newFriendId)        
            
def unfriendFromNetwork(users, friend1, friend2):        
    oldUser = users[friend1]
    if(oldUser.isFriend(friend2)):
        oldUser.removeFriend(friend2)

def addFriendToNetwork(users, friend1, friend2):
    oldUser = users[friend1]

    if(not oldUser.isFriend(friend2) and friend2 not in users):
        newUser = User(friend2)
        oldUser.addFriend(newUser)
        newUser.addFriend(oldUser)
        users[friend2] = newUser
                          
    if(not oldUser.isFriend(friend2) and friend2 in users):
        oldUser2 = users[friend2]
        oldUser.addFriend(oldUser2)
        oldUser2.addFriend(oldUser)

def printPurchases(users):
    for userid, userobj in users.items():
        print("")
        print("parent->" + userid)
        for friendid, friendobj in userobj.getFriends().items():
            print(friendid + " ", end="")

def buildNetwork(users, event_type, timestamp, id, id2="NA",amount=0.0):

    if(event_type == 'purchase'):
        if(id in users):
            oldUser = users[id]
            oldUser.addPurchase(timestamp, amount)
        else:
            newUser = User(id)
            users[id] = newUser
            newUser.addPurchase(timestamp, amount)
            
    if(event_type == 'befriend'):
        if(id in users):
            addFriendToNetwork(users,id,id2)
        elif(id not in users):
            newUser = User(id)
            users[id] = newUser
            addFriendToNetwork(users,id,id2)

    if(event_type == 'unfriend'):
        if(id in users):
            unfriendFromNetwork(users, id, id2)
        if(id2 in users):
            unfriendFromNetwork(users, id2, id)
            
def getNetworkPurchases(users, mainuser_id, userid, traverse_network, degree):
    
    Purchases = []
    if(degree > 0):  
        if (userid not in users):
            return;
        user = users[userid]
        for friendid,friendobj in user.getFriends().items():
            if(mainuser_id == friendid):
                continue
            if(traverse_network.__contains__(friendid)):
                continue
            traverse_network[friendid] = True
            Purchases = friendobj.getPurchases() + getNetworkPurchases(users, mainuser_id, friendid, traverse_network, degree-1)
            
    return Purchases    

def AnomalousPurchase(users, id, amount, logdata, flag, degree = 1, purchases = 1):
    
    traverse_network = defaultdict()
    networkPurchases = getNetworkPurchases(users,id,id,traverse_network, int(degree)) #get all the purchases in the network with a given degree
    networkPurchases.sort(key=lambda x:x['Timestamp'], reverse=True)     #sort the purchases in descending order
    networkPurchases = networkPurchases[:int(purchases)] # get first T purchases
    data = [float(k['Amount']) for k in networkPurchases] #get the list of all the amounts and calcualte the mean and standard deviation  
    mean = round(statistics.mean(data),2)
    pstdev = round(statistics.pstdev(data),2)

    # Selecting Anamalous Purchase using given condition (> 3 std. dev.)
    
    if (float(amount) > (mean + (3 * pstdev))):
        logdata.update({'mean':'%.2f'%mean,'sd':'%.2f'%pstdev})
        flag.write(json.dumps(logdata))
        flag.write("\n")

# users is updated for both data entry as well as anamalous purchases. Its created as a hash table.

users = defaultdict()

if (len(sys.argv) != 4):
    print('Please enter the Command in the linux terminal as: python ./src/process_log.py \
    ./log_input/batch_log.json \
    ./log_input/stream_log.json \
    ./log_output/flagged_purchases.json')
    sys.exit()

# Select the path of batch files, stream files and for storing the anamalous files

batch = sys.argv[1]
stream = sys.argv[2]
flaggedPurchases = sys.argv[3]    
    
first = True

# Opening Batch log File and Preparing a Network

with open(batch, 'r') as batchopen:
    for batchLine in batchopen:
        logdata = json.loads(batchLine)
        if(first):
            first = False
            degree = logdata['D']
            purchases = logdata['T']
            continue
        event_type = logdata['event_type']
        timestamp = logdata['timestamp']
        if(event_type == 'purchase'):
            id  = logdata['id']                
            id2 = 'NA'
            amount = logdata['amount']
        else:
            id  = logdata['id1']                
            id2 = logdata['id2']
            amount = 0.0
        buildNetwork(users,event_type,timestamp,id,id2,amount)         


# Opening Streaming File and identifying the Anamalous Purchases

with open(stream, 'r') as logInput, open(flaggedPurchases, 'w') as flag:
    for logfiles in logInput:
        logdata = json.loads(logfiles)
        event_type = logdata['event_type']
        if(event_type == 'purchase'):
            id = logdata['id']
            amount = logdata['amount']
            AnomalousPurchase(users,id,amount,logdata,flag,degree,purchases)

print('Flagging the Stream is done. Please check: flagged_purchases.json file')