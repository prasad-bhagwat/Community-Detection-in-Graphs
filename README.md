Community Detection in a Social Network graph
=====================================================

### Enviroment versions required:

Spark: 2.2.1  
Python: 2.7  
Scala: 2.11

### Dataset used for testing:
[MovieLens](https://grouplens.org/datasets/movielens/)   


### Python command for computing edge betweenness using Girvan-Newman algorithm

* * *

Computing edge betweenness using _“Prasad\_Bhagwat\_Betweenness.py”_ file

    spark-submit --driver-memory 4G --executor-memory 4G Prasad_Bhagwat_Betweenness.py <ratings file path>
    

where,  
_1. 'ratings file path'_ corresponds to the absolute path of input _‘ratings.csv’_ file  

Example usage of the above command is as follows:  

     ~/Desktop/spark-2.2.1/bin/spark-submit --driver-memory 4G --executor-memory 4G Prasad_Bhagwat_Betweenness.py ratings.csv


Note : _Output file_ named _‘Prasad\_Bhagwat\_Betweenness.txt’_ is generated at the location from where the _spark-submit_ is run.

### Scala command for computing edge betweenness using Girvan-Newman algorithm

* * *

Computing edge betweenness using _“Prasad\_Bhagwat\_Community_Detection.jar”_ file

    spark-submit --driver-memory 4G --executor-memory 4G --class Betweenness Prasad_Bhagwat_Community_Detection.jar <ratings file path>
    

where,  
_1. 'ratings file path'_ corresponds to the absolute path of input _‘ratings.csv’_ file  

Example usage of the above command is as follows:

     ~/Desktop/spark-2.2.1/bin/spark-submit --driver-memory 4G --executor-memory 4G --class Betweenness Prasad_Bhagwat_Community_Detection.jar ratings.csv
    

Note : _Output file_ named _‘Prasad\_Bhagwat\_Betweenness.txt’_ is generated at the location from where the _spark-submit_ is run.  

### Python command for Detection of Communities from input Graph

* * *

Detecting Communities from Graph using “Prasad_Bhagwat_Community.py”

     spark-submit --driver-memory 4G --executor-memory 4G Prasad_Bhagwat_Community.py <ratings file path>

where,  
_1. 'ratings file path'_ corresponds to the absolute path of input _‘ratings.csv’_ file  

Example usage of the above command is as follows:  

     ~/Desktop/spark-2.2.1/bin/spark-submit --driver-memory 4G --executor-memory 4G Prasad_Bhagwat_Community.py ratings.csv

Note : _Output file_ named _‘Prasad\_Bhagwat\_Community.txt’_ is generated at the location from where the _spark-submit_ is run.

### Scala command for Detection of Communities from input Graph using SparklingGraph library

* * *

Detecting Communities from Graph using _“Prasad\_Bhagwat\_Community_Detection.jar”_ file

     spark-submit --driver-memory 4G --executor-memory 4G --class Community_Sparkling Prasad_Bhagwat_Community_Detection.jar <ratings file path>

where,  
_1. 'ratings file path'_ corresponds to the absolute path of input _‘ratings.csv’_ file  

Example usage of the above command is as follows:  

     ~/Desktop/spark-2.2.1/bin/spark-submit --driver-memory 4G --executor-memory 4G --class Community_Sparkling Prasad_Bhagwat_Community_Detection.py ratings.csv

Note : _Output file_ named _‘Prasad\_Bhagwat\_Community_Sparkling.txt’_ is generated at the location from where the _spark-submit_ is run.

_Improvements which can be done:_  
_1. I have used epsilon = 0.8305 , but this value can be changed to generate better communities_  
