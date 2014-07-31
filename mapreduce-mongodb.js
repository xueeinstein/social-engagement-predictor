/**
 * do mapreduce to group MovieTweeting Dataset by {user_id, item_id}
 * @return recsys_grouped [collection]
 */
var mapFunction_by_useritem = function() {
    emit({
        user_id: this.user_id,
        item_id: this.item_id
    }, {
        tweets: [{
            _id: this._id,
            engagement: this.engagement,
            tweet_id: this.id,
            created_time: this.created_at_int,
            scrapint_time: this.scrapint_time
        }],
        count: 1
    });
};

var reduceFunction_by_useritem = function(key, values) {
    var groups = {
        count: 0,
        tweets: []
    };
    values.forEach(function(v) {
        groups.count += v.count;
        for (var i=0; i < v['tweets'].length; i++){
            groups.tweets.push({
                "_id": v['tweets'][i]['_id'],
                "engagement": v['tweets'][i]['engagement'],
                "tweet_id": v['tweets'][i]['tweet_id'],
                "created_time": v['tweets'][i]['created_time'],
                "scrapint_time": v['tweets'][i]['scrapint_time']
            });
        }
    });
    return groups;
};

var command = db.recsys.mapReduce(mapFunction_by_useritem,
    reduceFunction_by_useritem, {
        out: {
            merge: "recsys_grouped"
        },
        jsMode: false
    });

printjson(command);