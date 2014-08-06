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
    """ get tweet streams from different user"""
    streams = dict()
    tweet_nodes = dict()
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
    for user in group_byUser.find(timeout = False):
        user_id = int(user['_id'])
        streams[user_id] = user['value']['tweets']
        tmp_list = []
        for index, tweet in enumerate(streams[user_id]):
            tweet['_id'] = str(tweet['_id'])
            tweet['name'] = str(user_id) + '-' + str(index)
            tmp_list.append(tweet)
        # create user's tweet stream, representing in a path
        tmp_list = add_rel(tmp_list, 'next')
        path = neo4j.Path(*tmp_list)
        res = path.create(graph_db)

        # retrieve the node ID
        nodesID = retrieve_IDs(res)
        tweet_nodes[user_id] = nodesID

        # add labels
        add_labels(nodesID, 'tweet')
    return streams, tweet_nodes

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

def add_rel(tlist, rel):
    """ add relationship for tlist """
    tmp_list = []
    tmp_list.append(tlist[0])
    for i in tlist[1:]:
        tmp_list.append(rel)
        tmp_list.append(i)
    return tmp_list

def add_labels(nodes, *labels):
    """ add labels to nodes """
    graph_db = neo4j.GraphDatabaseService()
    for node in nodes:
        node_instance = neo4j.Node("http://localhost:7474/db/data/node/" + str(node))
        node_instance.add_labels(*labels)

def retrieve_IDs(res):
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
    group_byUser = group_tweets_byUser()
    # limit = 2
    # for i in group_byUser.find():
    #     print i, type(i), i['value']['count']
    #     if limit < 0:
    #         break
    #     limit = limit -1

    streams, tweet_nodes= get_tweet_streams(group_byUser)
    # limit = 3
    # for key, value in streams.items():
    #     print key, value, type(value[0]['_id']), str(value[0]['_id'])
    #     if limit < 0:
    #         break
    #     limit = limit - 1

    # f = open('out', 'w')
    # f.write(str(streams))
    # f.write("\n=========================\n")
    # f.write(str(tweet_nodes))
    # f.close()

    # read_the_dataset(filename, (0, 4))
    # users_set = get_users_set(tweets_list)
    # print users_set
    # grouped_tweets = get_grouped_users_tweets(users_set, tweets_list)
    # get_time_order_users_tweets(grouped_tweets, tweets_list)
    # print grouped_tweets
    