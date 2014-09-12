"""
>Note: 
1. There maybe error when execute query_nodes.stream(), because the JVM heap size is limited,
	so you need to manually modify the limitation of every query.
"""

from py2neo import neo4j
def exportSubNodes(query_nodes, filename, hasHeader = False):
	with open(filename, 'a') as nodes_file:
		if hasHeader == True:
			nodes_file.write('id,name,retweet_count,favorite_count,engagement\n')

		for node in query_nodes.stream():
			tmp_list = str(node[0]).split(' ')
			node_ID = tmp_list[0][1:]
			node_attr = eval(tmp_list[1][:-1])
			# print "node_ID:", node_ID
			# print "node_attr:", node_attr['name']
			line = node_ID+','+node_attr['name']+','+str(int(node_attr['retweet_count']))+','+\
				str(int(node_attr['favorite_count']))+','+str(int(node_attr['engagement']))+'\n'
			nodes_file.write(line)
		query_nodes.stream().close()
		
def getNodesSum(graph_db):
	query = neo4j.CypherQuery(graph_db, "MATCH (n) RETURN COUNT(n)")
	res = query.execute_one()
	return res

if __name__ == '__main__':
	filename = 'AllNodes.csv'
	graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")
	total_num = getNodesSum(graph_db)
	limitation = total_num / 15
	print limitation
	needed_loops = 15
	if limitation == 0:
		needed_loops = 0
		limitation = total_num
	elif total_num % 15 > 0:
		needed_loops = needed_loops + 1

	# first export 
	query_nodes = neo4j.CypherQuery(graph_db, "MATCH (n) RETURN n LIMIT " + str(limitation))
	exportSubNodes(query_nodes, filename, True)
	i = 1
	while i < needed_loops:
		query_nodes = neo4j.CypherQuery(graph_db, "MATCH (n) RETURN n SKIP " + \
			str(limitation * i) + " LIMIT " + str(limitation))
		exportSubNodes(query_nodes, filename)
		print str(limitation * i), '---', str(limitation * (i + 1)), 'DONE...'
		i = i + 1
