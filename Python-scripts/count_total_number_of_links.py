import pymongo
from multiprocessing import Queue, Process
client = pymongo.MongoClient('localhost', 27017)

#links_statistics would be a list of { "_id" : "10000", "out-degree" : 15 } objects
ls = client['wiki']['page_links']

#count links in records[begin...end] and put the partial sum
#into the thread-safe queue
def fromTo(begin, end, q):
    count = 0
    cur = ls.find()
    cur.skip(begin-1)
    for i in range(begin, end+1):
        count += len(cur.next()['links'])
    q.put(count)
    print('done from ' + str(begin) + ' to ' + str(end) + ', sub-count = ' + str(count))

if __name__ == '__main__':
    print('started counting total number of inter-page links...')
    total = 14313024
    step = int(14313024/128)+1
    q = Queue()
    processes = list(range(1, total, step))
    processes = list(map(lambda begin: (begin, min(begin + step - 1, total)), processes))
    processes = list(map(lambda beginEndTuple: Process(target = fromTo, args = beginEndTuple + (q,)), processes))
    map(lambda process: process.start(), processes)
    map(lambda process: process.join(), processes)
    print('finishing...collecting counts from queue')
    totalCount = 0
    while not q.empty():
        totalCount += q.get()
    print('tocalCount = ' + str(totalCount))
    
    
