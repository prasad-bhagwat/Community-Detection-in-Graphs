# Imports required for the program
from pyspark import SparkConf, SparkContext
from operator import add
from collections import deque
import time
import sys


# Computing BFS for given root node
def BFS_paths(user_graph, root):
    level_dict                          = dict()
    num_shortest_path_dict              = dict()
    parents_dict                        = dict()
    # Dictionary initializations
    for user in user_graph.keys():
        level_dict[user]                = 99999
        num_shortest_path_dict[user]    = 0
        parents_dict[user]              = set()
    # Initializing root node's values in all dictionaries
    level_dict[root]                    = 0
    num_shortest_path_dict[root]        = 1
    bfs_queue                           = deque()
    bfs_queue.append(root)

    while len(bfs_queue) > 0:
        current_node    = bfs_queue.pop()
        for next_node in user_graph[current_node]:
            if level_dict.get(next_node) == 99999:
                level_dict[next_node] = level_dict[current_node] + 1
                # Putting next_node node at first location of visited Queue
                bfs_queue.appendleft(next_node)
            # Getting shortest paths till root of the node
            if level_dict.get(next_node) == level_dict[current_node] + 1:
                num_shortest_path_dict[next_node] = num_shortest_path_dict[next_node] + num_shortest_path_dict[current_node]
                intermediate_set = parents_dict[next_node]
                if current_node is not None:
                    intermediate_set.add(current_node)
                    parents_dict[next_node] = intermediate_set

    # Check for items not equal to 99999
    return_level_dict = {key: value for (key, value) in level_dict.items() if value != 99999}
    return return_level_dict, num_shortest_path_dict, parents_dict


# Calculating betweenness values for the graph
def calculate_betweenness(corated_users_dict, root):
    # BFS Function call for each node as root
    level_dict, num_shortest_path_dict, parents_dict = BFS_paths(corated_users_dict, root)
    max_depth         = max(level_dict.values())
    betweenness_dict  = dict()
    # Looping from Max Depth to 0 for calculating betweenness
    for i in range(max_depth, 0, -1):
        nodes_below                 = {key: value for (key, value) in level_dict.items() if value == i}
        for node_below in nodes_below:
            nodes_up                = parents_dict[node_below]
            children_nodes          = corated_users_dict[node_below] - set(nodes_up)
            children_set            = [x for x in set(children_nodes) if level_dict[x] > i]
            nodes_up_shortest_paths = [num_shortest_path_dict[x] for x in nodes_up]
            num_shortest_paths      = sum(nodes_up_shortest_paths)
            # If current node is a leaf node direct split the node value based on number of shortest paths till parent
            if not children_set:
                result_dict         = {(node, node_below):\
                                        float(num_shortest_path_dict[node])\
                                        / num_shortest_paths for node in nodes_up}
                betweenness_dict.update(result_dict)
            # If current node is not a leaf node split the node value based on number of shortest paths till parent considering children credits
            else:
                children_credit     = sum([betweenness_dict.get((node_below, x), 1.0) for x in children_set])
                result_dict         = {(node, node_below):\
                                        (float(1.0 + children_credit) * float(num_shortest_path_dict[node]))\
                                        / num_shortest_paths for node in nodes_up}
                betweenness_dict.update(result_dict)
    return betweenness_dict.items()


# Main Function
def main():
    start_time          = time.time()

    # Command Line Arguments
    input_file          = sys.argv[1]

    # Output filename generation
    output_file_name    = "Prasad_Bhagwat_Betweenness.txt"
    output_file         = open(output_file_name, "w")

    # Creating Spark Context
    spark_config        = SparkConf()
    spark_context       = SparkContext(conf= spark_config)
    spark_context.setLogLevel("WARN")

    # Reading input training data file and extracting header
    input               = spark_context.textFile(input_file).filter(lambda x: "userId" not in x)
    input_data          = input.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1])))

    # Creating RDD of tuples like (user, movie) and dictionary of user: set(movies)
    user_movie_RDD      = input_data.map(lambda x: (x[0], x[1])).groupByKey().filter(lambda x : len(x[1]) >= 9).mapValues(set)
    user_movie_dict     = dict(user_movie_RDD.collect())

    # Creating RDD of tuples like ((user1, user2), corated movies) where corated movies >= 9
    users_corated_set   = set()
    for index1, (key1, value1) in enumerate(user_movie_dict.items()):
        for index2, (key2, value2) in enumerate(user_movie_dict.items()):
            if index2 > index1:
                if len(value1.intersection(value2)) >= 9:
                    users_corated_set.add((key1, key2))
                    users_corated_set.add((key2, key1))

    # Generating RDD and dictionary for given input graph
    users_corated_RDD   = spark_context.parallelize(users_corated_set).groupByKey().map(lambda x : (x[0], set(x[1]))).persist()
    corated_users_dict  = dict(users_corated_RDD.collect())

    # Generating betweenness values for all the edges in graph
    output_list = users_corated_RDD.keys().\
                    flatMap(lambda x: calculate_betweenness(corated_users_dict, x)).\
                    reduceByKey(add).\
                    filter(lambda x: x[0][0] < x[0][1]).\
                    sortByKey(True).\
                    collect()

    # Formatting output in expected form and writing to the output file
    output_result = "\n".join(map(str, output_list)).replace("((", "(").replace(")", "").replace("\n",")\n").replace(", ", ",")
    output_file.write(output_result + ")")
    output_file.close()

    # Printing time taken by the program
    print "Time:", int(time.time() - start_time), "sec"

# Entry point of the program
if __name__ == '__main__':
    main()
