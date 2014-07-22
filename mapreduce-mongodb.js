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
        created_time: this.created_at_int,
        scrapint_time: this.scrapint_time
    });
};

var reduceFunction_by_useritem = function(key, values) {
    var groups = {
        count: 0,
        tweets: []
    };
    for (v in values){
        groups.count += v.count;
        groups.tweets.push({
            "_id": v._id,
            "engagement": v.engagement,
            "tweet_id": v.tweet_id,
            "created_time": v.created_time,
            "scrapint_time": v.scrapint_time
        });
    }
    return groups;
};

var command = db.recsys.mapReduce(mapFunction_by_useritem,
    reduceFunction_by_useritem, {
        out: {
            merge: "recsys_grouped"
        },
        jsMode: true
    });

printjson(command);

