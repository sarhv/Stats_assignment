# Stats_assignment
Stats assignment - data analysis

**Objectives**

Our goal with the project is two fold:
  1. Design and build an ETL process to ingest and store this data in an efficient format for analysis
  2. Analyze the data to answer three below questions
 
      2.1 Which URL has the most ranks below 10 across all keywords over the period?
      
      2.2 Provide the set of keywords (keyword information) where the rank 1 URL changes the most over the period. A change, for the purpose of this question, is when a given keyword's rank 1 URL is different from the previous day's URL.
      
      2.3 We would like to understand how similar the results returned for the same keyword, market, and location are across devices. For the set of keywords, markets, and locations that have data for both desktop and smartphone devices, please devise a measure of difference to indicate how similar these datasets are, and please use this to show how the mobile and desktop results in our provided data set converge or diverge over time.

**####################################################################################**

**APPROACH**

For implementing this assignment, I had used the MAPR hadoop sandbox and VMWare player to run the sandbox image.

Part 1 : Storing the data in a MYSQL database.

**Install and configure mysql server, start the server**

 yum install mysql-server
 /etc/init.d/mysqld start 
 mysqladmin -u root password root123

**Create database and table structure**

mysql> CREATE DATABASE serp;
mysql> SHOW DATABASES;
mysql> USE serp;

mysql> create table serp_data(keyword varchar(20),market varchar(20),location varchar(50),device varchar(20),crawl_date date,rank int,URL varchar(500));

mysql> LOAD DATA INFILE '/user/root/serp.csv' INTO TABLE serp_data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

**Steps to install Sqoop and configure it**

###### //Copy paste com.mysql.jdbc.Driver into lib folder of sqoop found under /opt/mapr/sqoop/..

yum clean all    

yum install mapr-sqoop2-server

yum install mapr-sqoop2-client

maprcli node services -name sqoop2 -action start -nodes 192.166.227.129    ***//provide ip addresses of server nodes seperated by space

sqoop import --driver com.mysql.jdbc.Driver --connect jdbc:mysql://localhost:3306/serp --username root --password root --split-by crawl_date --table data --target-dir /user/root/hive_workspace2/

sqoop import --driver com.mysql.jdbc.Driver --connect jdbc:mysql://localhost:3306/serp --username root --password root -m 1 --table data --target-dir /user/root/hive_workspace2/

###### //since the table doesnt have a primary key, we cannot do parallel imports by mentioning a split-by column, thus we have to go with sequential import

###### //If any further analysis needs to be done in Hive QL, we have the option to directly import to Hive table, but here we do not need to.


Now, proceed to run the stats_wrapper.sh file

######## ./stats_wrapper.sh








