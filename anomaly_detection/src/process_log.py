class User():
    def __init__(self,name):
        self.name = name
        self.friends = []
        
    def addFriends(self,friends=[]):
        #input friends are user instaces, in list
        for friend in friends:
            if friend not in self.friends:
                self.friends.append(friend)
            
    def unFriends(self,friends=[]):
        #input friends are user instaces, in list
        for friend in friends:
            try:
                self.friends.remove(friend)
            except:
                pass
        
    def getFriends(self):
        #get a list of user's friend, in format of name strings
        return [friend.name for friend in self.friends]
    def _getFriends(self):
        #get a list of user's friend, in format of user instances
        return [friend for friend in self.friends]
    
    def addPurchase(self,purchase):
        #this is going to keep up to T recent records, with time stamp
        self.purchases.append(phurchase)
        
    def getFriendWithinDegreeD(self,D):
        #get list of friends within degree D, in format of name strings
        D = max(D,1)
        
        level = {self:0}
        visited = set([self])
        q = [self]
        while len(q) != 0:
            u = q.pop(0)
            v = u._getFriends()
            for vi in v:
                if vi not in visited:
                    q.append(vi)
                    level[vi] = level[u]+1
                    visited.add(vi)
            level_tmp = [l.name for l in level.keys()]
            
            degree = max(level.values())
            if degree >= D:
                break
        friends = visited.copy()
        friends.remove(self)
        tmp = [friend.name for friend in friends]
        return tmp

    
class Purchases(object):    
    def __init__(self,options={}):
        #options: various options in a dict format
        #example: options = {'purchaseLogging':'../log_output/recent_purchases.json', 
        #                    'anormaliesLogging': '../log_output/flagged_purchases.json',
        #                    'degreeOfInterest': 1,
        #                    'purchaseOfInterest': 10}
        
        if 'purchaseLogging' in options:
            self.purchaseLogging = options['purchaseLogging']
        else:
            self.purchaseLogging = '../log_output/recent_purchases.json'
        
        if 'anormaliesLogging' in options:
            self.anormaliesLogging = options['anormaliesLogging']
        else:
            self.anormaliesLogging = '../log_output/flagged_purchases.json'
            
        if 'degreeOfInterest' in options:
            self.degreeOfInterest = options['degreeOfInterest']
        else:
            self.degreeOfInterest = 1
            
        if 'purchaseOfInterest' in options:
            self.purchaseOfInterest = options['purchaseOfInterest']
        else:
            self.purchaseOfInterest = 10
            
        
    def addPurchase(self,purchase):
        #write purchase event into one file, each event is a dict format
        with open(self.purchaseLogging,'a') as f:
            f.write(str(purchase) + '\n')
            
    def findPurchases(self,users):
        #go into the purchase files, read from bottom up, find T records with id in users
        from file_read_backwards import FileReadBackwards
        from ast import literal_eval
        
        f = FileReadBackwards(self.purchaseLogging)
        
        T = max(2,self.purchaseOfInterest)
        noRecords = 0
        purchases = []
        
        for l in f:
            if noRecords >= T:
                break
            ldict = literal_eval(l)   
            if ldict['id'] in users:
                purchases.append(ldict)
                noRecords += 1
        
        return purchases
                
        
    def meanPurchases(self,purchases):
        #compute mean
        amountSum = 0
        noPurchases = len(purchases)
        if noPurchases == 0:
            return 0
        for purchase in purchases:
            amountSum += float(purchase['amount'])
        return amountSum/noPurchases
    
    def stdPurchases(self,purchases):
        #compute std
        mean = self.meanPurchases(purchases)
        squaredError = 0 
        noPurchases = len(purchases)
        if noPurchases == 0:
            return 0
        for purchase in purchases:
            squaredError += (float(purchase['amount']) - mean)**2
        #print squaredError,noPurchases
        return np.sqrt(squaredError/noPurchases)
        
    def detectAnormalies(self,purchase,usersList):
        #for a specific purchase, detect if it is normal
        user = [u for u in usersList if u.name == purchase['id']][0]
        friends = user.getFriendWithinDegreeD(self.degreeOfInterest)
        ReferencePurchases = self.findPurchases(friends)
        mean = self.meanPurchases(ReferencePurchases)
        std = self.stdPurchases(ReferencePurchases)
        normalRange = [mean-3*std, mean+3*std]
        
        if float(purchase['amount']) < normalRange[0]:
            Flag = 1
        elif float(purchase['amount']) > normalRange[1]:
            Flag = 1
        else:
            Flag = 0
        
        #write into loggingFile
        if Flag == 1:
            purchase['mean'] = "{0:.2f}".format(mean)
            purchase['sd'] = "{0:.2f}".format(std)
            record = [(k,purchase[k]) for k in ('event_type','timestamp','id','amount','mean','sd')]
            record = OrderedDict(record)
            #record = OrderedDict({k:purchase[k] for k in ('event_type','timestamp','id','amount','mean','sd')})
            with open(self.anormaliesLogging,'a') as f:
                #f.write(str(record) + '\n')
                json.dump(record,f)
            print 'Find a purchase considered as an anomaly:\n',purchase
            print
            
        return Flag

def initialize(filename,flagfile):
    #read history transations from filename
    options = {'purchaseLogging':'log_output/recent_purchases.json',
           'anormaliesLogging': 'log_output/flagged_purchases.json',
           'degreeOfInterest': 1,
           'purchaseOfInterest': 10}

    options['anormaliesLogging'] = flagfile
    
    with open(filename) as f:
        l = f.readline()
        ldict = literal_eval(l)
        if 'D' in ldict:
            options['degreeOfInterest'] = ldict['D']
        if 'T' in ldict:
            options['purchaseOfInterest'] = ldict['T']

    
    purchases = Purchases(options)   
    
    userList = []
    userIdList = set([])
    with open(filename) as f:
        next(f)
        for l in f:
            ldict = literal_eval(l)
            if ldict['event_type'] == 'purchase':
                purchases.addPurchase(ldict)
            if ldict['event_type'] == 'befriend':
                userId1 = ldict['id1']
                userId2 = ldict['id2']
                if userId1 not in userIdList:
                    user1 = User(userId1)
                    userList.append(user1)
                    userIdList.add(userId1)
                if userId2 not in userIdList:
                    user2 = User(userId2)
                    userList.append(user2)
                    userIdList.add(userId2)
                user1.addFriends([user2])
                user2.addFriends([user1])
            if ldict['event_type'] == 'unfriend':
                user1 = [u for u in userList
                        if u.name == ldict['id1']][0]
                user2 = [u for u in userList
                        if u.name == ldict['id2']][0]
            
                user1.unFriends(['id2'])
                user2.unFriends(['id1'])
      
    return purchases, userList, userIdList

def stream(filename,purchases,userList,userIdList):
    #streaming data, detect and log anormalies
    with open(filename) as f:
        l = f.readline()
        ldict = literal_eval(l)
        if ldict['event_type'] == 'purchase':
            purchases.addPurchase(ldict)
            flag = purchases.detectAnormalies(ldict,userList)

        if ldict['event_type'] == 'befriend':
            
            userId1 = ldict['id1']
            userId2 = ldict['id2']
            if userId1 not in userIdList:
                user1 = user('userId1')
                userList.append(user1)
                userIdList.add(userId1)
            if userId2 not in userIdList:
                user2 = user('userId2')
                userList.append(user2)
                userIdList.add(userId2)
            user1.addFriends([user2])
            user2.addFriends([user1])
            
        if ldict['event_type'] == 'unfriend':
            user1 = [u for u in userList
                    if u.name == ldict['id1']][0]
            user2 = [u for u in userList
                    if u.name == ldict['id2']][0]
            user1.unFriends(['id2'])
            user2.unFriends(['id1'])
        


import sys,json
from file_read_backwards import FileReadBackwards
from ast import literal_eval
from collections import OrderedDict
import numpy as np

batchlog,streamlog,flagfile = sys.argv[1:]

userList = []
print ".........initializing........."
purchases, userList, userIdList = initialize(batchlog,flagfile)

print "...........streaming.........."
stream(streamlog,purchases,userList,userIdList)
 