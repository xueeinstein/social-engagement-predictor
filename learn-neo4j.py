from py2neo import neo4j
graph = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
from py2neo import node, rel
map = graph.create(
	 node(name='Sara'),
	 node(name='Maria'),
	 node(name='Steve'),
	 node(name='John'),
	 node(name='Joe'),
	 rel(3, 'friend', 0),
	 rel(3, 'friend', 4),
	 rel(0, 'friend', 1),
	 rel(4, 'friend', 2),
)
from py2neo import cypher
session = cypher.Session("http://localhost:7474")
tx = session.create_transaction()

tx.append("MATCH (john {name: 'John'})-[:friend]->()-[:friend]->(fof)"
			"RETURN john, fof")
tx.execute()

tx.append("MATCH (user)-[:friend]->(follower)"
			"WHERE user.name IN ['Joe', 'John', 'Sara', 'Maria', 'Steve'] AND follower.name =~ 'S.*'"
			"RETURN user, follower.name")


data = {
  "props" : [ {
    "position" : "Developer",
    "awesome" : True,
    "name" : "Andres"
  }, {
    "position" : "Developer",
    "name" : "Michael",
    "children" : 3
  } ]
}

tx.append("CREATE (n:Person " + str(data) +") "
			"RETURN n")
tx.execute()

tx.commit()