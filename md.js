/**
 * do mapreduce to group MovieTweeting Dataset by {user_id, item_id}
 * @return recsys_grouped [collection]
 */
var mapFunction_by_user = function() {
    emit(
        { user_id: this.user_id } , {
            tweets: [{
                _id: this._id,
                favorite_count: this.favorite_count,
                retweet_count: this.retweet_count,
                engagement: this.engagement,
                tweet_id: this.id,
                created_time: this.created_at_int,
                scrapint_time: this.scrapint_time
            }],
            count: 1
        });
};

var reduceFunction_by_user = function(key, values) {
    var groups = {
        count: 0,
        tweets: []
    };
    values.forEach(function(v) {
        groups.count += v.count;
        for (var i = 0; i < v['tweets'].length; i++) {
            groups.tweets.push({
                "_id": v['tweets'][i]['_id'],
                "favorite_count": v['tweets'][i]['favorite_count'],
                "retweet_count": v['tweets'][i]['retweet_count'],
                "engagement": v['tweets'][i]['engagement'],
                "tweet_id": v['tweets'][i]['tweet_id'],
                "created_time": v['tweets'][i]['created_time'],
                "scrapint_time": v['tweets'][i]['scrapint_time']
            });
        }
    });
    return groups;
};

var finalizeFunction_sortby_createTime = function(key, reduceVal) {
    reduceVal['tweets'].sort(function(a, b) {
        return a['created_time'] - b['created_time']
    });
    return reduceVal;
}

var command = db.recsys.mapReduce(
    mapFunction_by_user,
    reduceFunction_by_user, {
        out: {
            merge: "recsys_grouped"
        },
        finalize: finalizeFunction_sortby_createTime,
        jsMode: true
    });

printjson(command);