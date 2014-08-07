import json
import pymongo
import datetime
import time
from bson.code import Code
from py2neo import neo4j, node

def read_the_training(the_dataset_file, limitation):
    """ read training.dat into MongoDB, in limitation """
    header = True
    down, up = limitation
    i = 0
    client = pymongo.MongoClient()
    db = client['test']
    collection = db['recsys']
    recsys = db.recsys
    with open(the_dataset_file,'r') as infile:
        for line in infile:
            if header:
                header = False
                continue # Skip the CSV header line
            if i < down:
                i = i + 1
                continue
            if i >= up:
                break
            i = i + 1
            line_array = line.strip().split(',')
            rating = int(line_array[2])
            if rating > 10:
                continue
            user_id = int(line_array[0])
            item_id = int(line_array[1])
            scrapint_time = int(line_array[3])
            tweet = ','.join(line_array[4:]) # The json format also contains commas
            json_obj = json.loads(tweet) # Convert the tweet data string to a JSON object
            # Use the json_obj to easy access the tweet data
            # e.g. the tweet id: json_obj['id']
            # e.g. the retweet count: json_obj['retweet_count']
            
            # filter tweet which rating > 10
            json_obj['user_id'] = user_id
            json_obj['item_id'] = item_id
            json_obj['rating'] = rating
            json_obj['scrapint_time'] = scrapint_time
            json_obj['engagement'] = json_obj['retweet_count'] + json_obj['favorite_count']
            tmp = datetime.datetime.strptime(json_obj['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
            json_obj['created_at_int'] = int(totimestamp(tmp))
            recsys.insert(json_obj)
   
def group_tweets_byUser():
    """ Through the mongodb supported MapReduce to group tweets by user_id """
    mapper = Code("""
                  function(){
                    emit(this.user_id , {
                        tweets: [{
                                _id: this._id,
                                item_id: this.item_id,
                                favorite_count: this.favorite_count,
                                retweet_count: this.retweet_count,
                                engagement: this.engagement,
                                tweet_id: this.id,
                                created_time: this.created_at_int,
                                scrapint_time: this.scrapint_time
                            }],
                        count: 1
                    });
                  }
                  """)
    reducer = Code("""
                   function (key, values) {
                    var groups = {
                        count: 0,
                        tweets: []
                    };
                    values.forEach(function(v) {
                        groups.count += v.count;
                        for (var i = 0; i < v['tweets'].length; i++) {
                            groups.tweets.push({
                                "_id": v['tweets'][i]['_id'],
                                "item_id": v['tweets'][i]['item_id'],
                                "favorite_count": v['tweets'][i]['favorite_count'],
                                "retweet_count": v['tweets'][i]['retweet_count'],
                                "engagement": v['tweets'][i]['engagement'],
                                "tweet_id": v['tweets'][i]['tweet_id'],
                                "created_time": v['tweets'][i]['created_time'],
                                "scrapint_time": v['tweets'][i]['scrapint_time']
                            });
                        }
                    });
                    groups['tweets'].sort(function(a, b){
                            return a['created_time'] - b['created_time']
                        });
                    return groups;
                   }
                   """)
    client = pymongo.MongoClient()
    db = client['test']
    group_byUser = db.recsys.map_reduce(mapper, reducer, "recsys_group_1")
    return group_byUser

def get_tweet_streams(group_byUser):
    """ 
    get tweet streams from different user
    @group_byUser, the mongodb cursor
    """
    streams = dict()
    user_path = dict()
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    for user in group_byUser.find(timeout = False):
        user_id = int(user['_id'])
        streams[user_id] = user['value']['tweets']
        tmp_list = []
        for index, tweet in enumerate(streams[user_id]):
            tweet['_id'] = str(tweet['_id'])
            tweet['name'] = str(user_id) + '-' + str(index)
            tmp_list.append(tweet)

        streams[user_id] = [int(i['item_id']) for i in streams[user_id]]
        # create user's tweet stream, representing in a path
        nodesID = add_path(tmp_list, 'next')
        user_path[user_id] = nodesID

        # add labels
        add_labels(nodesID, 'tweet')
    return streams, user_path

def get_movies_set(movies_file_name):
    """ from all tweets collection, get movies set """
    # get all avaliable movie data
    movies_dict = dict()
    movie_tags = set()
    with open(movies_file_name, 'r') as movies_file:
        for line in movies_file:
            tmp_list = line.split('::')
            movie = dict()
            item_id = int(tmp_list[0])
            movie['movie_name'] = tmp_list[1][:-7]
            movie['movie_year'] = tmp_list[1][-5:-1]
            movie['movie_categories'] = tmp_list[2]
            for tag in tmp_list[2].split('|'):
                movie_tags.add(tag)
            movies_dict[item_id] = movie

    # filter and get the movie in traning dataset
    client = pymongo.MongoClient()
    db = client['test']
    recsys = db.recsys
    for tweet in recsys.find(timeout = False):
        item_id = tweet['item_id'] 
        if item_id in movies_dict.keys():
            movies_dict[item_id]['exist'] = 1
        else:
            # movies_dict[item_id] = {'movie_name': '', 'movie_year': '', 'movie_categories': ''}
            raise
    for i in movies_dict.keys():
        if movies_dict[i]['exist'] == 1:
            del movies_dict[i]['exist']
        else:
            del movies_dict[i]

    # save movies & movie_tags nodes in neo4j DB
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    movie_tags_dictlist = [{'cate_name': i} for i in list(movie_tags)] 
    del movie_tags
    movie_tags_IDs = add_nodes(movie_tags_dictlist, 'cate_name', graph_db, 'category')
    del movie_tags_dictlist

    movies_dictlist = [{'item_id': i, 'movie_name': j['movie_name'], 'movie_year': j['movie_year'],\
        'movie_categories': j['movie_categories']} for i, j in movies_dict.items()]
    del movies_dict
    movie_IDs = add_nodes(movies_dictlist, 'item_id', graph_db, 'movie')
    # save move & category rels
    get_movie_tags_rel(movies_dictlist, movie_tags_IDs, movie_IDs)
    del movies_dictlist

    return movie_IDs, movie_tags_IDs

def get_tweet_movie_rel(streams, user_path, movie_IDs):
    """ get non-retweeted tweets and movie relationship """
    graph_db = neo4j.GraphDatabaseService()
    for user_id in streams:
        for index, item_id in enumerate(streams[user_id]):
            tweet_node_instance = neo4j.Node("http://localhost:7474/db/data/node/"+\
                str(user_path[user_id][index]))
            movie_node_instance = neo4j.Node("http://localhost:7474/db/data/node/"+\
                str(movie_IDs[item_id]))
            graph_db.create((tweet_node_instance, 'rated', movie_node_instance))

def get_movie_tags_rel(movies, movie_tags_IDs, movie_IDs):
    """ get move & category rel """
    graph_db = neo4j.GraphDatabaseService()
    for movie in movies:
        item_id = movie['item_id']
        movie_node_instance = neo4j.Node("http://localhost:7474/db/data/node/"+\
            str(movie_IDs[item_id]))
        movie_tags = movie['movie_categories'].split('|')
        for tag in movie_tags:
            cate_node_instance = neo4j.Node("http://localhost:7474/db/data/node/"+\
                str(movie_tags_IDs[tag]))
            graph_db.create(movie_node_instance, 'is', cate_node_instance)

def get_retweet_rel():
    """ get retweet rel from training dataset, we treat mentions are from source tweet """
    pass

def get_users_set(tweets_list):
    users_set = set()
    for i in range(len(tweets_list)):
        users_set.add(tweets_list[i][0])
    return users_set

def get_grouped_users_tweets(users_set, tweets_list):
    grouped_users_tweets = {}
    # initial the dict of grouped_users_tweets
    for user in users_set:
        grouped_users_tweets[user] = []
    print "created users size",len(grouped_users_tweets)
    for i in range(len(tweets_list)):
        grouped_users_tweets[tweets_list[i][0]].append(i)

    return grouped_users_tweets

def get_time_order_users_tweets(grouped_tweets, tweets_list):
    """ get time ordered tweet 
        returned users_set like this: {user_id: [t1, t2, [t3_time1, t3_time2], t4]}
    """
    # reorder the tweets of one user by time
    
    ## reorder the tweets of one user by time
    for key, value in grouped_tweets.iteritems():
        # group by tweet_id
        tmp_tweetid_dict= collections.defaultdict(list)
        for i in value:
            tmp_tweetid_dict[tweets_list[i][4]['id']].append(i)

        value = [ j for tweet_id, j in tmp_tweetid_dict.items()]
        del grouped_tweets[key]
        grouped_tweets[key] = value

        # for i in value:
            # if type(i) == type([]) and len(i) > 1:
                # print "user_id: ", key, "items", i
        # group by scrapint_time

def totimestamp(dt, epoch=datetime.datetime(1970,1,1)):
    td = dt - epoch
    # return td.total_seconds()
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 1e6 

def add_path(tlist, rel):
    """ add path for tlist """
    tmp_list = []
    tmp_list.append(tlist[0])
    for i in tlist[1:]:
        tmp_list.append(rel)
        tmp_list.append(i)
    path = neo4j.Path(*tmp_list)
    res = path.create(graph_db)
    return retrieve_pathNodes_IDs(res)

def add_nodes(nlist, prime_key, graph_db, *labels):
    """ add nodes of every element of nlist """
    res = dict()
    query_res = graph_db.create(*nlist)
    for key, val in enumerate(query_res):
        val = val.split(' ')
        node_ID = int(val[0][1:])
        node_info = eval(val[1][:-1])
        res[node_info[prime_key]] = node_ID

    if labels != ():
        nodes = [i for i in res.values()]
        add_labels(nodes, *labels)
    return res

def add_labels(nodes, *labels):
    """ add labels to nodes """
    graph_db = neo4j.GraphDatabaseService()
    for node in nodes:
        node_instance = neo4j.Node("http://localhost:7474/db/data/node/" + str(node))
        node_instance.add_labels(*labels)

def retrieve_pathNodes_IDs(res):
    """ according to path created result to retrieve nodes IDs """
    tmp_list = []
    if list(res) == []:
        # only one node
        res = str(res)
        res = int(res[1:-1])
        tmp_list.append(res)
    else:
        res = list(res)
        res = [str(i) for i in res]
        tmp_set = set()
        for i in res:
            ID_str = i.split('-')
            start = int(ID_str[0][1:-1])
            tmp_set.add(start)
            end = int(ID_str[2][2:-1])
            tmp_set.add(end)
        tmp_list = list(tmp_set)
        tmp_list.sort()

    return tmp_list

if __name__ == "__main__":
    # save training.dat into MongoDB
    # filename = raw_input("Please input the filename: ")
    # if filename == '':
    #     filename = 'read-test.dat'
    # slices = [(i*10000, (i+1)*10000) for i in range(18)]
    # for i in slices:
    #     read_the_training(filename, i)
    #     print "read limitation:", i, "OK!"

    # test group tweets by user_id
    # group_byUser = group_tweets_byUser()
    # limit = 2
    # for i in group_byUser.find():
    #     print i, type(i), i['value']['count']
    #     if limit < 0:
    #         break
    #     limit = limit -1

    # streams, user_path= get_tweet_streams(group_byUser)
    # limit = 3
    # for key, value in streams.items():
    #     print key, value, type(value[0]['_id']), str(value[0]['_id'])
    #     if limit < 0:
    #         break
    #     limit = limit - 1

    # f = open('out', 'w')
    # f.write(str(streams))
    # f.write("\n=========================\n")
    # f.write(str(user_path))
    # f.close()

    get_movies_set("movies.dat")    