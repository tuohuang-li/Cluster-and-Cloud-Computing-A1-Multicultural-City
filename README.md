# **Multicultural City**
***
COMP90024 Cluster and Cloud Computing Assignment 1


Your task in this programming assignment is to implement a simple, parallelized application leveraging the University
of Melbourne HPC facility SPARTAN. Your application will use a large Twitter dataset and a grid/mesh for Sydney
to identify the languages used in making Tweets. Your objective is to count the number of different languages used for
tweets in the given cells and the number of tweets in those languages and hence to calculate the multicultural nature of
Sydney!

# Abstract
It has been brought to the attention that a huge amount of information extracted from textural
content on social media such as Twitter can be used to better understand our society. In this
project, we have done research on the usage of languages when tweeting and calculated the
top-10 popular languages among 16 grids in Sydney, Australia.

# The aim of the project
The project aims to find the frequent occurrence of languages being used, total number of tweets
and total number of languages used in each grid in Sydney. This parallel program is implemented
on Spartan, which is a high performance computing (HPC) facility provided by the University of
Melbourne. In this project, we also compared the performance when using different running
resources, including 1 node 1 core, 1 node 8 core and 2 node 8 core.

# The dataset
The dataset used in this project is “bigTwitter”, a json file containing 4,057,523 rows of tweets.
To locate the tweets, only tweets including geo-coordinates will be extracted. Another file named
“sydGrids.json” provides the grid data which divides the whole Sydney range into 16 identical
squares.

![alt text](https://github.com/tuohuang-li/COMP90024_A1/blob/master/sydGrid.jpg?raw=true)

(Picture 1 - sydGrid)

# How to run
On Sbartan 
Use sbatch 1c1n.slurm for 1-node-1-core, 1n8c.slurm for 1-node-8-cores, and 2n8c.slurm for 2-nodes-8-cores.
