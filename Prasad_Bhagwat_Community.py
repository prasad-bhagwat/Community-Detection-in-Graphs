# Imports required for the program
from pyspark import SparkConf, SparkContext
from operator import add
from collections import deque
import networkx
from itertools import combinations
from networkx import connected_components
from operator import itemgetter
import time
import sys


# Computing BFS for given root node
def BFS_paths(user_graph, root):
    level_dict             	     = dict()
    num_shortest_path_dict 	     = dict()
    parents_dict           	     = dict()
    # Dictionary initializations
    for user in user_graph.keys():
        level_dict[user]             = 99999
        num_shortest_path_dict[user] = 0
        parents_dict[user]           = set()
    # Initializing root node's values in all dictionaries
    level_dict[root]                 = 0
    num_shortest_path_dict[root]     = 1
    visited_queue                    = deque()
    visited_queue.append(root)

    while len(visited_queue) > 0:
        current_node                 = visited_queue.pop()
        for next_node in user_graph[current_node]:
            if level_dict.get(next_node) == 99999:
                level_dict[next_node] = level_dict[current_node] + 1
                # Putting next_node node at first location of visited Queue
                visited_queue.appendleft(next_node)
            # Getting shortest paths till root of the node
            if level_dict.get(next_node) == level_dict[current_node] + 1:
                num_shortest_path_dict[next_node] = num_shortest_path_dict[next_node] + num_shortest_path_dict[current_node]
                parent_set                        = parents_dict[next_node]
                if current_node is not None:
                    parent_set.add(current_node)
                    parents_dict[next_node]       = parent_set

    # Check for items not equal to 99999
    return_level_dict = {key: value for (key, value) in level_dict.items() if value != 99999}
    return return_level_dict, num_shortest_path_dict, parents_dict


# Calculating betweenness values for the graph
def calculate_betweenness(corated_users_dict, root):
    # BFS Function call for each node as root
    level_dict, num_shortest_path_dict, parents_dict = BFS_paths(corated_users_dict, root)
    max_depth        = max(level_dict.values())
    betweenness_dict = dict()
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
                result_dict         = {(node, node_below): \
                                   	float(num_shortest_path_dict[node]) \
                                   	/ num_shortest_paths for node in nodes_up}
                betweenness_dict.update(result_dict)
                # If current node is not a leaf node split the node value based on number of shortest paths till parent considering children credits
            else:
                children_credit     = sum([betweenness_dict.get((node_below, x), 1.0) for x in children_set])
                result_dict         = {(node, node_below): \
                                   	(float(1.0 + children_credit) * float(num_shortest_path_dict[node])) \
                                   	/ num_shortest_paths for node in nodes_up}
                betweenness_dict.update(result_dict)
    return betweenness_dict.items()


# Calculating modularity value for the given community
def calculate_modularity(corated_users_dict, degree_dict, denominator_2_m, community):
    sum_list = list()
    for member in set(combinations(set(community), 2)):
        # for member in product(set(community), set(community)):
        # If edge is present in the graph
        if member[1] in corated_users_dict[member[0]]:
            sum_list.append((1.0 - degree_dict[member[0]] * degree_dict[member[1]] / denominator_2_m))
        # If edge is not present in the graph
        else:
            sum_list.append((0.0 - degree_dict[member[0]] * degree_dict[member[1]] / denominator_2_m))
    # Returning modularity of input community
    return sum(sum_list) / denominator_2_m


# Processing each partition of graph for faster modularity calculations
def process_partition(edge_list_RDD,
              edge_betweenness_list,
              user_graph,
              corated_users_dict,
              degree_dict,
              denominator_2_m):

    edge_list = list(edge_list_RDD)
    # Looping over all the edges in decreasing order of betweenness values
    for edge in edge_betweenness_list:
        if edge != edge_list[0]:
            # Removing edges from graph in decreasing order of betweenness values
            # till current partition's first edge is found
            user_graph.remove_edge(edge[0], edge[1])
        else:
            # Found first edge of the current partition
            break

    # Calculating communities and Modularity value corresponding to previous partition
    previous_community   = list(connected_components(user_graph))
    result_community     = [previous_community[i] for i in range(len(previous_community))]
    graph_modularity     = 0.0
    for community in previous_community:
        graph_modularity += calculate_modularity(corated_users_dict, degree_dict, denominator_2_m,
                                               list(community))

    # Looping over edges of the current partition
    for edge in edge_list:
        # Removing edge from graph in decreasing order of betweenness values
        user_graph.remove_edge(edge[0], edge[1])
        components_list = list(connected_components(user_graph))
        # Calculating Modularity if communities are formed after removing the edge from graph
        if len(previous_community) < len(components_list):
            new_modularity = 0.0
            for community in list(components_list):
                new_modularity += calculate_modularity(corated_users_dict, degree_dict, denominator_2_m,
                                                       list(community))
            # Finding maximum Modularity value and corresponding communities for the current partition
            if float(new_modularity) >= float(graph_modularity):
                graph_modularity = new_modularity
                result_community = [components_list[i] for i in range(len(components_list))]
            previous_community   = components_list
    yield graph_modularity, result_community


# Main Function
def main():
    # Recording time taken by the program
    start_time            = time.time()

    # Command Line Arguments
    input_file            = sys.argv[1]

    # Output filename generation
    output_file_name      = "Prasad_Bhagwat_Community.txt"
    output_file           = open(output_file_name, "w")

    # Creating Spark Context
    spark_config          = SparkConf()
    spark_context         = SparkContext(conf=spark_config)
    spark_context.setLogLevel("WARN")

    # Reading input training data file and extracting header
    input                 = spark_context.textFile(input_file).filter(lambda x: "userId" not in x)
    input_data            = input.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1])))

    # Creating RDD of tuples like (user, movie) and dictionary of user: set(movies)
    user_movie_RDD        = input_data.map(lambda x: (x[0], x[1])).groupByKey().filter(lambda x: len(x[1]) >= 9).mapValues(set)
    user_movie_dict       = dict(user_movie_RDD.collect())

    # Creating RDD of tuples like ((user1, user2), corated movies) where corated movies >= 9
    users_corated_set = set()
    for index1, (key1, value1) in enumerate(user_movie_dict.items()):
        for index2, (key2, value2) in enumerate(user_movie_dict.items()):
            if index2 > index1:
                if len(value1.intersection(value2)) >= 9:
                    users_corated_set.add((key1, key2))
                    users_corated_set.add((key2, key1))

    # Generating RDD and dictionary for given input graph
    users_corated_RDD     = spark_context.parallelize(users_corated_set).groupByKey().map(lambda x: (x[0], set(x[1]))).persist()
    corated_users_dict    = dict(users_corated_RDD.collect())

    # Generating betweenness values for all the edges in graph
    output_list           = users_corated_RDD.keys(). \
        			flatMap(lambda x: calculate_betweenness(corated_users_dict, x)). \
        			reduceByKey(add). \
        			filter(lambda x: x[0][0] < x[0][1]). \
        			sortByKey(True). \
        			collect()

    output_dict           = dict(output_list)

    # Getting betweenness values in descending order
    edge_betweenness      = sorted(output_dict, key=output_dict.get, reverse=True)
    edge_betweenness_RDD  = spark_context.parallelize(edge_betweenness)


    # Calculating denominator for modularity calculation
    denominator_2_m       = len(output_list) * 2.0
    degree_dict           = {key: float(len(value)) for (key, value) in corated_users_dict.items()}

    # Using Networkx to generate graph out of given connected edges
    user_graph            = networkx.Graph()
    user_graph.add_edges_from(list(users_corated_set))

    # Calling MapPartitions for calculating Modularity of partitions
    result_community      = edge_betweenness_RDD.mapPartitions(lambda x: process_partition(x,
                                                                              edge_betweenness,
                                                                              user_graph,
                                                                              corated_users_dict,
                                                                              degree_dict,
                                                                              denominator_2_m)).\
                                                                            collect()

    result_community_dict = dict(result_community)
    community_list 	  = list()
    for item in result_community_dict.get(max(result_community_dict.keys())):
        community_list.append(sorted(list(item)))

    # Finding Communities with maximum Modularity and formatting output in the expected format
    output_result         = "\n".\
                    		join(map(str, sorted(community_list, key=itemgetter(0)))).replace(", ", ",")

    # Writing results to the output file
    output_file.write(output_result)
    output_file.close()

    # Printing time taken by the program
    print "Time:", int(time.time() - start_time), "sec"


# Entry point of the program
if __name__ == '__main__':
    main()
