/**
 * do mapreduce to group MovieTweeting Dataset by tweet_id
 * @return recsys_grouped [collection] in format:
 *  "_id":tweet_id , "value": {"_id":[obj1, obj2, ...]}
 */
var mapFunction_by_tweet_id = function() {
    emit(this.id, {
        count: 1,
        _id: this._id
    });
};

var reduceFunction_by_tweet_id = function(key, values) {
    var groups = {
        count: 0,
        tweets: []
    };
    values.forEach(function(value) {
        groups.count += value.count;
        groups.tweets.push(value._id);
    });
    return groups;
};

db.recsys.mapReduce(mapFunction_by_tweet_id,
    reduceFunction_by_tweet_id, {
        out: {
            merge: "recsys_grouped"
        },
        jsMode: true
    })

/**
 * do mapreduce to group MovieTweeting Dataset by {user_id, item_id}
 * @return recsys_grouped [collection]
 */
var mapFunction_by_useritem = function() {
    emit({
        user_id: this.user_id,
        item_id: this.item_id
    }, {
        count: 1,
        _id: this._id,
        engagement: this.engagement,
        tweet_id: this.id,
        scrapint_time: this.scrapint_time
    });
};

var reduceFunction_by_useritem = function(key, values) {
    var groups = {
        count: 0,
        tweets: []
    };
    values.forEach(function(value) {
        groups.count += value.count;
        groups.tweets.push({
            "_id": value._id,
            "engagement": value.engagement,
            "tweet_id": value.id,
            "scrapint_time": value.scrapint_time
        })
    });
    return groups;
};

db.recsys.mapReduce(mapFunction_by_useritem,
    reduceFunction_by_useritem, {
        out: {
            merge: "recsys_grouped"
        },
        jsMode: true
    })

