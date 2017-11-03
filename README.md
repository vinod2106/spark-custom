# spark-custom
Spark examples. 

#Command to run code.
spark-submit --class com.shavinod.spark.funcs.SearchExample \
--master local --deploy-mode client \
spark-custom-1.0-SNAPSHOT-jar-with-dependencies.jar ads.csv
