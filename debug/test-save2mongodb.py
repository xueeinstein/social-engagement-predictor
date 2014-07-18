import pymongo
import datetime

client = pymongo.MongoClient()
db = client['test']
collection = db['posts']
print collection

post = {"author": "Bill", "text": "Just for Test", "tags": ["mongodb", "python", "pymongo"], \
	"date": datetime.datetime.utcnow()}
print post
print "========", type(post), "========"
posts = db.posts
# post_id = posts.insert(post)
# print post_id
for i in posts.find():
	print i