# hdfs-streaming
# Steps to run the appliaction

HDFS container set up:
Run the following commands:

 - cd <path_to_docker-compose file>
 - docker-compose up -d
 - docker exec -it hdfs-namenode bash
 - docker-compose down

# Create hdfs path
hdfs dfs -mkdir -p /home/data

# List the directory
hdfs dfs -ls /home/data

# Build the project
mvn clean

# Run the project
This will upload the file to hdfs path

# Dataset URL
 - https://catalog.data.gov/dataset/


# Useful commands
 - hdfs dfs -cat /home/data/file.csv | wc -l
 - hdfs dfs -rm /home/data/file.csv
 - hdfs dfs -rm -r /home/data
 - hdfs dfs -tail /home/data/file.csv

