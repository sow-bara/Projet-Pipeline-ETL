# Projet-Pipeline-ETL

sbt clean compile
sbt run
spark-submit --class scala.Main --master local[*] target/scala-2.12/project_scala_spark_2.12-0.1.0.jar /home/lemir/IdeaProjects/project_scala_spark/datasets/events/events.json /home/lemir/IdeaProjects/project_scala_spark/output

