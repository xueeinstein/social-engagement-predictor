import json
import collections
import pymongo
import datetime
import time
# import matplotlib.pyplot as plt 

def read_the_dataset(the_dataset_file, limitation):
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

if __name__ == "__main__":
    filename = raw_input("Please input the filename: ")
    if filename == '':
        filename = 'read-test.dat'
    slices = [(i*10000, (i+1)*10000) for i in range(18)]
    for i in slices:
        read_the_dataset(filename, i)
        print "read limitation:", i, "OK!"
    # read_the_dataset(filename, (0, 4))
    # users_set = get_users_set(tweets_list)
    # print users_set
    # grouped_tweets = get_grouped_users_tweets(users_set, tweets_list)
    # get_time_order_users_tweets(grouped_tweets, tweets_list)
    # print grouped_tweets
    