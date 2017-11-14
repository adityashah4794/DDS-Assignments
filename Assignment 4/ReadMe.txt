							Approach

Mapper: Reads from the file loaded into HDFS line by line and creates a key-value pair using the integer field and the tuple

Reducer: Based on the table-name we split the tuples into two using the oolean flag to detect the first table-name and accordingly put them in the respective table. Then using the join-key if there exists record in both the tuples, we append them together, if they exist in only one of the tuples, it is ignored. This handles the scenario of not having a join repeat again. 

Driver: Driver is the functon which calls the required classes and gets the input direcotry and optput directory from the input arguments and runs the code. The result file can be downlaoded from the hadoop localhost webpage.
