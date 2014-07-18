import json
import collections
# import matplotlib.pyplot as plt 

def read_the_dataset(the_dataset_file):
    tweets = list()
    header = True
    with file(the_dataset_file,'r') as infile:
        for line in infile:
            if header:
                header = False
                continue # Skip the CSV header line
            line_array = line.strip().split(',')
            user_id = int(line_array[0])
            item_id = int(line_array[1])
            rating = int(line_array[2])
            scraping_time = line_array[3]
            tweet = ','.join(line_array[4:]) # The json format also contains commas
            json_obj = json.loads(tweet) # Convert the tweet data string to a JSON object
            # Use the json_obj to easy access the tweet data
            # e.g. the tweet id: json_obj['id']
            # e.g. the retweet count: json_obj['retweet_count']
            tweets.append((user_id, item_id, rating, scraping_time, json_obj))
    return tweets
   
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
        

if __name__ == "__main__":
    filename = raw_input("Please input the filename: ")
    if filename == '':
        filename = 'read-test.dat'
    tweets_list = read_the_dataset(filename)
    # print tweets_list[0][4]['id']
    # print tweets_list[0]
    # print tweets_list 
    # print type(tweets_list)
    users_set = get_users_set(tweets_list)
    print users_set
    grouped_tweets = get_grouped_users_tweets(users_set, tweets_list)
    get_time_order_users_tweets(grouped_tweets, tweets_list)
    print grouped_tweets
    # x = range[5]
    # y = []
    # n = [17310, 17745, 17985, 18219, 21081]
    # y = [i.['favorite_count']+tweets_list[][4][i].['retweet_count'] for i in n]
