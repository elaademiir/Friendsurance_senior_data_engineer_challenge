# REPO STRUCTURE
```console
friendsurance_case
|
|
|-- checkpointlocation 
|-- data_simulation
|   |--sample_data
|   |  |---customer.csv
|   |  |---exchange_rate.csv
|   |  |---transaction.csv
|   |--src
|      |---exchange_rate_producer.py
|      |---transaction_producer.py_
|-- env
|   |---docker-compose.yml
|   |---requirements.txt
|-- log
|   |---debug
|   |---error
|-- mysql
|   |---sql_commands.sql
|   |---case_one_result.csv
|   |---case_two_result.csv
|-- src
|   |---case_one_solution.py
|   |---case_two_solution.py
|-- utils
|   |---repo_logger.py
|   |---tools.py
|-- config.yml
|-- README.md
|-- senior_de_challenga.md
```

### Explanation of Folders

- **checkpointlocation folder:** it is for spark commits.
- **data simulation folder:** it helps to simulate generating Kafka Messages with csv data sources. So two scripts (*/data_simulation/src/exchange_rate_producer.py & /data_simulation/src/transaction_producer.py*) read csv's from sample_data folder and send data to Kafka topics.
- **env folder:** I run MySQL, Kafka and Zookeeper services with docker-compose.yml file. I use to build my python environment with requirements.txt.
- **log folder:** Log folder is generated automatically even if it is deleted. Log folder is created by utils as soon as the repo is running. So I can find my debug logs or error logs in log folders.
- **mysql folder:** You can view my system architectures, output screenshots, output csv's, views and DDL's in /mysql/ folder.
- **src folder:** it contains solutions of case 1 and case 2. These python scripts were written in PySpark Streaming.
- **utils folder:** it contains repo_logger.py and tools.py files. Repo_logger.py helps you to see error and debug logs in log folder. Tools.py file helps to read your configuration variables from config.yml. The best part of it is that you set once in config.yml with singleton pattern and all repo reads the same value.
- **config.yml file:** it contains configuration values.

# BEFORE start, GET READY

Please be sure that you are in the project path in order to run commands in your CLI.

1. Create a conda environment.
```console
$ conda create --name friendsurance_case python==3.8
$ conda activate friendsurance_case
$ pip install -r ./env/requirements.txt
```

2. Run docker compose file. 

I run MySQL, Kafka and Zookeeper in Docker. Because I use MacOS, I added "platform: linux/x86_64" command in MySQL service as you see in docker-compose file. If you're not use MacOS, please remove it. Then run commands below:

```console
$ cd env/
$ docker-compose up -d
```

Note: If your computer does not have Docker and Docker-compose, commands will fail.

Create a database which will be called as "friendsurance".

3. This repo reads config.yml to extract configuration variables. 

4. Add your repo path to /utils/tools.py line 22. This helps you to read all configuration values with a Singleton method in config.yml.

4. Run command below.

    $ conda develop /PATH/TO/YOUR/FOLDER/WITH/MODULES (for example: conda develop /Users/eodemir/Documents/GitHub/Friendsurance_senior_data_engineer_challenge)

5. Please set your JAVA_HOME, SPARK_HOME to your environment before running Spark, if you need.

# LETS START!

## CASE 1 - SOLUTION

Please view my system architecture for Case 1 (/mysql/case_one/system_architecture_case1.png)

Run: 
- /src/case_one_solution.py
- /data_simulation/src/transaction_producer.py

!!! You can find results and screenshots in the folder: /mysql/case_one/ !!!

## CASE 2 - SOLUTION
Please view my system architecture for Case 2 (/mysql/case_one/system_architecture_case2.png)

Run 
- /src/case_two_solution.py
- /data_simulation/src/exchange_rate_producer.py

!!! You can find results and screenshots in the folder: /mysql/case_two/ !!!

# WHAT WOULD I DO IF I HAD MORE THAN 4 DAYS?

- I would implement Airflow to run my scripts.
- I would create powerful dashboards to show you my outputs in Tableau. I am addicted to visualization tools such as Kibana and ELK, Apache Superset, Power BI...
- I would use Windowing in Spark.
- I would change my "write_to_mysql" function with Jdbc connector in Spark. I would not use SQL query in my code and prefer to use methods.
- I would use primary_key and design a data modelling for sure!

I really enjoyed! Thank you for this opportunity. See you soon! :)

Best Regards,
ELA DEMIR


