from __future__ import print_function
from math import sqrt
from collections import defaultdict
import sys
import json
import statistics
from statistics import pstdev

class User:
    
    def __init__(self, id):
        self.id = id
        #dictionary to hold first degree friends
        self.friends = defaultdict()
        #list to hold the purchases made by this particular user
        self.purchases = []

    def getId(self):
        return self.id
    
    def getFriends(self):
        return self.friends
    
    def addFriend(self, newFriend):
        if(not self.friends.__contains__(newFriend.getId())):
            self.friends[newFriend.getId()] = newFriend

    def removeFriend(self, oldFriend):        
        if(oldFriend in self.friends):
            self.friends.__delitem__(oldFriend)
    
    def isFriend(self, newFriendId):
        return self.friends.__contains__(newFriendId)        
            
    def addPurchase(self, timestamp, amount):
        self.purchases.append({'Timestamp': timestamp, 'Amount': amount})

    def printPurchases(self):
        print(self.purchases)

    def getPurchases(self):
        return self.purchases
   
def getNetworkPurchases(users, mainUserId, userId, traversedNetwork, degree):
  
    #recursively get all the purchases in the network
    allPurchases = []
    if(degree > 0):  
        if (userId not in users):
            return;
        user = users[userId]
        for friendid,friendobj in user.getFriends().items():
            if(mainUserId == friendid):
                continue
            if(traversedNetwork.__contains__(friendid)):
                continue
            traversedNetwork[friendid] = True
            allPurchases = friendobj.getPurchases() + getNetworkPurchases(users, mainUserId, friendid, traversedNetwork, degree-1)
            
    return allPurchases        
            

def addFriendToNetwork(users, friend1, friend2):
    oldUser = users[friend1]
    #if id2 is not a friend of id1
    #and is a new user
    if(not oldUser.isFriend(friend2) and friend2 not in users):
        newUser = User(friend2)
        oldUser.addFriend(newUser)
        newUser.addFriend(oldUser)
        users[friend2] = newUser
    #if id2 is not a friend of id1
    #and is an old existing user                        
    if(not oldUser.isFriend(friend2) and friend2 in users):
        oldUser2 = users[friend2]
        oldUser.addFriend(oldUser2)
        oldUser2.addFriend(oldUser)

def unfriendFromNetwork(users, friend1, friend2):        
    oldUser = users[friend1]
    #if id2 is a friend
    if(oldUser.isFriend(friend2)):
        oldUser.removeFriend(friend2)

def printPurchases(users):
    for userid, userobj in users.items():
        print("")
        print("parent->" + userid)
        for friendid, friendobj in userobj.getFriends().items():
            print(friendid + " ", end="")

def buildPurchaseNetwork(users, event_type, timestamp, id, id2="NA",amount=0.0):

    if(event_type == 'purchase'):
        if(id in users):
            oldUser = users[id]
            oldUser.addPurchase(timestamp, amount)
        else:
            newUser = User(id)
            newUser.addPurchase(timestamp, amount)
            users[id] = newUser

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

def findAnomalousPurchase(users, id, amount, degree, purchases, logData, flaggedHandler):
    traversedNetwork = defaultdict()

    #get all the purchases in the network with a given degree
    networkPurchases = getNetworkPurchases(users,id,id,traversedNetwork, int(degree))

    #sort the purchases in descending order
    networkPurchases.sort(key=lambda x:x['Timestamp'], reverse=True)

    #get first T purchases
    networkPurchases = networkPurchases[:int(purchases)]

    #get the list of all the amounts
    #and calcualte the mean and standard deviation    
    data = [float(k['Amount']) for k in networkPurchases]
    mean = round(statistics.mean(data),2)
    pstdev = round(statistics.pstdev(data),2)

    #flag the purchase if the amount is greater than
    #mean+(3*standard deviation)
    if (float(amount) > (mean + (3 * pstdev))):
        logData.update({'mean':'%.2f'%mean,'sd':'%.2f'%pstdev})
        flaggedHandler.write(json.dumps(logData))
        flaggedHandler.write("\n")

            
def main(argv):
          
    #hash table to store Users
    users = defaultdict()
      
    if len(argv) is not 3:    
        print('python ./src/process_log.py ../log_input/batch_log.json ../log_input/stream_log.json ../log_output/flagged_purchases.json')        
        sys.exit(2)
    
    batchFile = argv[0]
    streamFile = argv[1]
    flaggedPurchasesFile = argv[2]
    
    firstLine = True
    degree = 1
    purchases = 2
    #read the batch file and build a network (n-ary tree)
    with open(batchFile, 'rb') as batchInput:
        for batchLine in batchInput:
            logData = json.loads(batchLine)
            #get degree and number of purchases allowed
            if(firstLine):
                firstLine = False
                degree = logData['D']
                purchases = logData['T']
                continue
            event_type = logData['event_type']
            timestamp = logData['timestamp']
            if(event_type == 'purchase'):
                id = logData['id']
                id2 = "NA"
                amount = logData['amount']
                buildPurchaseNetwork(users,event_type,timestamp,id,id2,amount)
            elif(event_type == 'befriend'):
                id = logData['id1']                
                id2 = logData['id2']                
                amount = 0.0
                buildPurchaseNetwork(users,event_type,timestamp,id,id2,amount)         
            elif(event_type == 'unfriend'):
                id = logData['id1']                
                id2 = logData['id2']                
                amount = 0.0
                buildPurchaseNetwork(users,event_type,timestamp,id,id2,amount)         

    #read the stream file
    with open(streamFile, 'rb') as logInput, open(flaggedPurchasesFile, 'w') as flaggedHandler:
        for streamLine in logInput:
            logData = json.loads(streamLine)
            event_type = logData['event_type']
            if(event_type == 'purchase'):
                id = logData['id']
                amount = logData['amount']
                findAnomalousPurchase(users,id,amount,degree,purchases,logData,flaggedHandler)

                    
if __name__=="__main__":
    main(sys.argv[1:])       