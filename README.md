# Social Engagement Predictor

## About

This is a project for [RecSys Challenge 2014](http://2014.recsyschallenge.com). However, it has already out of date before I finish this project.

## Main Idea

Not like other traditional collaborative filtering recommendation system, I try to find the real connection relationship by mining the [MovieTweeting](https://github.com/xueeinstein/xueeinstein.github.com/blob/master/assets/doc/crowdrec2013_Dooms.pdf) dataset. And then tracks the Twitter user community hot engagement. According to this result, model and predict engagement ranking list.

## Review

Because of the large dataset, I couldn't handle it directly on my 4G memory laptop :( . So I built a tedious system. Firstly, import the dataset into MongoDB, and the using MapReduce which integrates with MongoDB grouped and filtered the useful data. To handle a two hundred thousand nodes and millions of relationship tweets graph, I chose [Neo4j](http://www.neo4j.org). It is awesome, especially its cypher language. 

Now, I'm trying [gephi](https://gephi.org) to visualize it.

