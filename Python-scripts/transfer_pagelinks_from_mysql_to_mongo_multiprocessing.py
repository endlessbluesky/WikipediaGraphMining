import MySQLdb
import pymongo
from multiprocessing import Process
import stop_watch

######helper methods######
def get_page_id_from_title(title, mongo_page_info_collection):
    entry = mongo_page_info_collection.find_one({'page_title': title, 'page_namespace': 0})
    if entry == None:
        return None
    else:
        return int(mongo_page_info_collection.find_one({'page_title': title})['page_id'])
#helper methods ended


#(in parallel) traverse through all pages
#and transfer those in namespace 0 to mongo database
#total number of pages: 32877338
#i.e., 1 <= begin <= end <= 32877338
#number of processes distributed: 16
#below is the function that processes all pages
#in comprehensive_page_info_reproduced
#having index >= begin and <=end
def process_pages_from_to(begin, end):
    #get mysql database connections and cursors of type SSCursor
    print('started processing this interval: ' + repr((begin, end)))
    mysql_connection_1 = MySQLdb.connect(host = 'localhost',
                                   user = 'gaoyuan',
                                   passwd = 'gaoyuan',
                                   db = 'gaoyuan_wikipedia')
    mysql_connection_1.cursorclass = MySQLdb.cursors.SSCursor
    mysql_page_info_cursor = mysql_connection_1.cursor()

    mysql_connection_2 = MySQLdb.connect(host = 'localhost',
                                   user = 'gaoyuan',
                                   passwd = 'gaoyuan',
                                   db = 'gaoyuan_wikipedia')
    mysql_pagelinks_cursor = mysql_connection_2.cursor()

    #get mongo database connection
    client = pymongo.MongoClient('localhost', 27017)
    mongo_pagelinks_graph = client['wiki']['reproduced_page_links_id_to_id_graph_in_namespace_0']
    mongo_page_info_collection = client['wiki']['comprehensive_page_info_reproduced']

    #in this context counter starts from 1 (till TOTAL_NUMBER_OF_PAGES)
    mysql_page_info_cursor.execute('SELECT * FROM page LIMIT'
                                   + ' ' + str(end-begin+1)
                                   + ' ' + 'OFFSET' + ' ' + str(begin-1))

    #now each entry is of the form (page_id, page_title)
    for page_info_entry in mysql_page_info_cursor:
        #print('now processing: title = ' + page_info_entry[2] + ', id = ' + str(page_info_entry[0]))
        #pass if it is not in the main namespace
        if page_info_entry[1] != 0:
            continue
        #otherwise, get the ids of all its main-namespace links
        page_id = page_info_entry[0]
        mysql_pagelinks_cursor.execute('SELECT pl_title FROM pagelinks WHERE pl_from=' + str(page_id)
                                       + ' ' + 'AND pl_namespace=0')
        list_of_page_link_ids = filter(lambda x: x!=None,
                                       map(lambda x: get_page_id_from_title(x[0], mongo_page_info_collection),
                                           mysql_pagelinks_cursor.fetchall()))
        mongo_pagelinks_graph.insert({'page_id': int(page_id), 'links': list_of_page_link_ids})
        #mysql_pagelinks_cursor.close()
        
    mysql_connection_1.close()
    print('done with this interval: ' + repr((begin, end)))

##same methods, time measure added
##def process_pages_from_to_with_timer(begin, end):
##    #get mysql database connections and cursors of type SSCursor
##    stop_watch.reset()
##    stop_watch.start()
##    mysql_connection_1 = MySQLdb.connect(host = 'localhost',
##                                   user = 'gaoyuan',
##                                   passwd = 'gaoyuan',
##                                   db = 'gaoyuan_wikipedia')
##    mysql_connection_1.cursorclass = MySQLdb.cursors.SSCursor
##    mysql_page_info_cursor = mysql_connection_1.cursor()
##
##    mysql_connection_2 = MySQLdb.connect(host = 'localhost',
##                                   user = 'gaoyuan',
##                                   passwd = 'gaoyuan',
##                                   db = 'gaoyuan_wikipedia')
##    mysql_pagelinks_cursor = mysql_connection_2.cursor()
##
##    #get mongo database connection
##    client = pymongo.MongoClient('localhost', 27017)
##    mongo_pagelinks_graph = client['wiki']['page_links_id_to_id_graph_from_namespace_0']
##    mongo_page_info_collection = client['wiki']['comprehensive_page_info_reproduced']
##
##    #in this context counter starts from 1 (till TOTAL_NUMBER_OF_PAGES)
##    mysql_page_info_cursor.execute('SELECT page_id, page_namespace, page_title FROM page LIMIT'
##                                   + ' ' + str(end-begin+1)
##                                   + ' ' + 'OFFSET' + ' ' + str(begin-1))
##    stop_watch.stop()
##    print('total time for creating connections and fetching the batch of pages = ' + str(stop_watch.getTimeElapsedInSeconds()))
##    stop_watch.reset()
##    
##    #now each entry is of the form (page_id, page_title)
##    for page_info_entry in mysql_page_info_cursor:
##        print('now processing: title = ' + page_info_entry[2] + ', id = ' + str(page_info_entry[0]))
##        #pass if it is not in the main namespace
##        if page_info_entry[1] != 0:
##            continue
##        #otherwise, get the ids of all its main-namespace links
##        page_id = page_info_entry[0]
##        stop_watch.reset()
##        stop_watch.start()
##        mysql_pagelinks_cursor.execute('SELECT pl_title FROM pagelinks WHERE pl_from=' + str(page_id)
##                                       + ' ' + 'AND pl_namespace=0')
##        stop_watch.stop()
##        print('time for mysql_pagelinks_cursor to execute pagelinks query = ' + str(stop_watch.getTimeElapsedInSeconds()))
##        stop_watch.reset()
##        stop_watch.start()
##        tuple_of_page_titles = map(lambda x: x[0], mysql_pagelinks_cursor.fetchall())
##        print(tuple_of_page_titles)
##        for page_title in tuple_of_page_titles:
##            page_id = get_page_id_from_title(page_title, mongo_page_info_collection)
##            print(page_id)
##        #list_of_page_link_ids = map(lambda x: get_page_id_from_title(x[0], mongo_page_info_collection), mysql_pagelinks_cursor.fetchall())
##        stop_watch.stop()
##        #print(list_of_page_link_ids)
##        print('time for mapping link titles to id = ' + str(stop_watch.getTimeElapsedInSeconds()))
##        stop_watch.reset()
##        stop_watch.start()
##        mysql_pagelinks_cursor.close()
##        stop_watch.stop()
##        print('time for closing mysql_pagelinks_cursor = ' + str(stop_watch.getTimeElapsedInSeconds()))
##
##    stop_watch.reset()
##    stop_watch.start()
##    mysql_page_info_cursor.close()
##    mysql_connection_1.close()
##    stop_watch.stop()
##    print('time for closing major cursor and connection: ' + str(stop_watch.getTimeElapsedInSeconds()))

if __name__ == '__main__':
    print('started constructing the id-id graph')
    print('started!')
    total = 32877738
    step = int(total/16) + 1
    processes = list(range(1, total, step))
    processes = map(lambda begin: (begin, min(begin + step - 1, total)), processes)
    processes = map(lambda beginEndTuple: Process(target = process_pages_from_to, args = beginEndTuple), processes)

    #######trial######
    #process_pages_from_to(12,12)
    ######end_of_trial######

    map(lambda p: p.start(),processes)
    map(lambda p: p.join(),processes)
    
