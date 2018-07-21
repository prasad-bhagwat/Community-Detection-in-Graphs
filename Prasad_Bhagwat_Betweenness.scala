// Imports required for the program
import java.io._
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}


object Betweenness{
  // Computing BFS for given root node
  def BFS_paths(user_graph: Map[Int, Iterable[Int]], root: Int): (Map[Int, Int], Map[Int, Int], Map[Int, Set[Int]]) = {
    var level_dict              = Map[Int, Int]()
    var num_shortest_path_dict  = Map[Int, Int]()
    var parents_dict            = Map[Int, Set[Int]]()
    // Dictionary initializations
    for (user <- user_graph.keys)
    {
      level_dict             += (user -> Int.MaxValue)
      num_shortest_path_dict += (user -> 0)
      parents_dict           += (user -> Set())
    }
    // Initializing root node's values in all dictionaries
    level_dict             += (root -> 0)
    num_shortest_path_dict += (root -> 1)
    var bfs_queue          = List(root)

    while (bfs_queue.nonEmpty)
    {
      val current_node = bfs_queue.last
      bfs_queue        = bfs_queue.dropRight(1)
      val nodes_set    = user_graph(current_node).toSet
      for (next_node <- nodes_set)
      {
        if (level_dict(next_node) == Int.MaxValue)
        {
          level_dict   += (next_node -> (level_dict(current_node) + 1))
          // Putting next_node node at first location of visited Queue
          bfs_queue    = next_node::bfs_queue
        }
        // Getting shortest paths till root of the node
        if (level_dict(next_node) == level_dict(current_node) + 1)
        {
          num_shortest_path_dict += (next_node -> (num_shortest_path_dict(next_node) + num_shortest_path_dict(current_node)))
          var intermediate_set = parents_dict(next_node)
          if (current_node != Int.MaxValue)
	  {
            intermediate_set     += current_node
            parents_dict         += (next_node -> intermediate_set)
          }
        }
      }
    }
    // Check for items not equal to 99999
    (level_dict.filter(x => x._2 != Int.MaxValue), num_shortest_path_dict, parents_dict)
  }


  // Calculating betweenness values for the graph
  def calculate_betweenness(corated_users_dict: Map[Int, Iterable[Int]], root: Int): Map[(Int, Int), Double] ={
    // BFS Function call for each node as root
    val (level_dict, num_shortest_path_dict, parents_dict) = BFS_paths(corated_users_dict, root)
    val max_depth         = level_dict.values.max
    var betweenness_dict  = Map[(Int, Int), Double]()
    // Looping from Max Depth to 0 for calculating betweenness
    for (i <- max_depth to 1 by -1) 
    {
      val nodes_below     	    = level_dict.filter(_._2 == i)
      for (node_below <- nodes_below.keys) 
      {
        val nodes_up       	    = parents_dict(node_below)
        val children_nodes          = corated_users_dict(node_below).toSet.diff(nodes_up)
        val children_set            = children_nodes.filter(x => level_dict(x) > i)
        val nodes_up_shortest_paths = new ListBuffer[Int]()
        for (x <- nodes_up)
        {
          nodes_up_shortest_paths   += num_shortest_path_dict(x)
        }
        val num_shortest_paths  = nodes_up_shortest_paths.sum
        // If current node is a leaf node direct split the node value based on number of shortest paths till parent
        if (children_set.isEmpty) 
        {
          val result_dict  = nodes_up.map(node_up => {
            ((node_up, node_below), num_shortest_path_dict(node_up).toDouble / num_shortest_paths)
          }).toMap
          betweenness_dict = betweenness_dict ++ result_dict
        }
        // If current node is not a leaf node split the node value based on number of shortest paths till parent considering children credits
        else 
        {
          val children_path_credit = new ListBuffer[Double]()
          for (x <- children_set)
          {
            children_path_credit   += betweenness_dict.getOrElse((node_below, x), 1.toDouble)
          }
          val children_credit      = children_path_credit.sum
          val result_dict          = nodes_up.map(node_up => {
            ((node_up, node_below), ((1.toDouble + children_credit) * num_shortest_path_dict(node_up).toDouble) / num_shortest_paths)
          }).toMap
          betweenness_dict         = betweenness_dict ++ result_dict
        }
      }
    }
    betweenness_dict
  }


  // Main Function
  def main(args: Array[String]) {
    val start_time = System.nanoTime()

    // Command Line Arguments
    val input_file = args(0)

    // Output filename generation
    val output_file_name    = "Prasad_Bhagwat_Betweenness.txt"
    val output_file         = new PrintWriter(new File(output_file_name))

    // Creating Spark Context
    val spark_config        = new SparkConf()
    val spark_context       = new SparkContext(spark_config)
    spark_context.setLogLevel("WARN")

    // Reading input training data file and extracting header
    val input               = spark_context.textFile(input_file).filter(x => ! x.contains("userId"))
    val input_data          = input.map( x => {
      val y = x.split(",")
      (y(0).toInt, y(1).toInt)
    })

    // Creating RDD of tuples like (user, movie) and dictionary of user: set(movies)
    val user_movie_RDD      = input_data.map(x => (x._1, x._2)).groupByKey().filter(x => x._2.size >= 9)
    val user_movie_dict     = user_movie_RDD.collect().toMap

    // Creating RDD of tuples like ((user1, user2), corated movies) where corated movies >= 9
    val users_corated_set   = scala.collection.mutable.Set[(Int, Int)]()
    for (user1_movie <- user_movie_dict)
    {
      for (user2_movie <- user_movie_dict)
      {
        if (user1_movie._1 < user2_movie._1)
        {
          val user_intersection = user1_movie._2.toSet.intersect(user2_movie._2.toSet)
          if (user1_movie._2.toSet.intersect(user2_movie._2.toSet).size >= 9)
          {
            users_corated_set   += ((user1_movie._1, user2_movie._1))
            users_corated_set   += ((user2_movie._1, user1_movie._1))
          }
        }
      }
    }

    // Generating RDD and dictionary for given input graph
    val users_corated_RDD   = spark_context.parallelize(users_corated_set.toSeq).groupByKey().persist()
    val corated_users_dict  = users_corated_RDD.collect().toMap

    // Generating betweenness values for all the edges in graph
    val output_list         = users_corated_RDD.keys.
                        	flatMap(x => calculate_betweenness(corated_users_dict, x)).
                        	reduceByKey(_+_).
                        	filter(x => x._1._1 < x._1._2).
                        	sortByKey(true).
                        	collect()

    // Formatting output in expected form and writing to the output file
    val intermediate_result = output_list.mkString("\n")
    var output_result       = intermediate_result.replace("((", "(").replace(")", "").replace("\n",")\n")
    output_result           = output_result.concat(")")

    output_file.write(output_result)
    output_file.close()

    // Printing time taken by program
    println("Time: "+ ((System.nanoTime() - start_time) / 1e9d).toInt + " sec")
  }
}
