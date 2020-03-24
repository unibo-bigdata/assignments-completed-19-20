# 201 MapReduce

Module 1, Big Data course (81932), University of Bologna.

## 201-1 Instructions to compile and run a MapReduce job

The old way:
```shell
# Get the job file from the virtual cluster's HDFS
hdfs dfs –get /bigdata/mapreduce/wordcount/WordCount.java
# Compile the Java source file and create the jar
# Notice: the command hadoop classpath returns the class path(s) needed to get the Hadoop jar and the required libraries
javac -classpath $(hadoop classpath) -d . WordCount.java
jar cf wc.jar WordCount*.class
# OR: directly get the jar
hdfs dfs –get /bigdata/mapreduce/wordcount/wc.jar mapreduce/wordcount
```

The new way:

- Pull this assignment's code
- Compile with ```./gradlew``` (remember to set the Java version depending on where you intend to run the job)
- Move the jar into the virtual machine (simple copy/paste works) or into your cluster machine (use WinSCP or simply SCP from command line)

To run the job, use ```hadoop jar <jarFile> <MainClass> <inputDir> <outputDir> [params]```, where

- <jarFile> is the local path to the jar
- <MainClass> is the name of the class with the Main you want to run (e.g., "exercise1.WordCount")
- <inputDir> is the existing directory on HDFS that contains the input files (e.g., "/bigdata/dataset/sample-input")
- <outputDir> is the directory on HDFS to be created by the job to store the results (e.g., "mapreduce/wordcount/output")
- [params] are the optional parameters
- Full example: ```hadoop jar BD-201-mapreduce.jar exercise1.WordCount /bigdata/dataset/sample-input mapreduce/wordcount/output```

## 201-2 Compile and run the first MapReduce jobs

Goal: modify the source code of the WordCount job to add the use of the Combiner; try out the WordLengthCount

Compile and run the job on the capra and divinacommedia datasets. To check the output use ```hdfs dfs -cat mapreduce/output/* | head -n 30```. Try to answer the following questions.

- How much time does it take to run the jobs?
- How many mappers and reducers have been instantiated?
- How is the output sorted?
- What happens if we enforce 0 reducers?

Check the code of exercise2.WordLengthCount.

- Instead of counting the number of times that a word appears, we want to count how many words are there with a given length. 
- If the number of reducers is not given, it defaults to 1

Run the job and verify the output as above.

## 201-3 Testing combiners

Goal: modify the source code in package exercise3 to try out combiners. The dataset for this exercise is ```weather-sample```.

- Add a combiner to the existing job
  - Notice: the combiner is implemented as a reducer, but its output must be of the same type of the map output
  - How does the performance improve?
- Modify the job to get the average temperature in each month of each year
  - Which combiner strategy can we adopt (if any)?

## 201-4 AverageWordLength and InvertedIndex 

Goal: complete the two exercises in package exercise4. The datasets for this exercise are ```capra``` and ```divinacommedia```.

- AverageWordLength aims to calculate the average length of the words based on their initial letter
  - Getting the first letter: ```substring()```
  - Getting the length: ```getLength()```
- InvertedIndex aims to derive, for each word, the list of offsets in which they appear
  - Map: cast the key to a ```LongWritable```
  - Reduce: use a ```TreeSet<Long>``` and print it with the ```toString()``` method
  - In case of ```java.io.IOException: Type mismatch```:
    - Verify the type of keys and values in Mapper and Reducer classes
    - Declare the same types in the job object (```setMapOutputKeyClass```, ```setOutputKeyClass```, etc.)
