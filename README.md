# concurrency_framework
Framework for processing work concurrently via Snowflake JS stored proc 


[Snowflake stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures-overview.html) use JavaScript to express complex logic. As of version 4.32.1, Snowflake stored procedures do not support a multithreading for parallel or concurrent execution, i.e. a snowflake stored procedures can not start additional processes. At least not directly. Luckily this doesn't mean, we couldn't execute a workfload in parallel. The sample code here shows how a single stored procedure can initiate parallel code execution by using [Snowflake Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro.html). The idea is to partition the total amount of work into N batches/chunks. Each batch of work will be handled by 1 task. Multiple tasks can execute on a single cluster. Snowflake will automatically scale out when queuing occurs after MAX_CONCURRENCY_LEVEL (see below) is reached. Cross process communiction can be accomplished by messages in a logging table.

The sample code takes 4 input parameters.
* method name ('PROCESS_REQUEST') 
* number of worker processes
* number of statements per batch
* name of inputput table 

The input table provides the SQL statements to be executed. It has an ID (statement_id) and a JSON document in a variant column. The actual sql statement is defined in an array called sqlquery. As in the example below, you can submit multiple sql statements within the arrary. 
```
create or replace table meta_schema.statements as 
select 
seq4() statement_id
,parse_json ('{"TableDB":"CONCURRENCY_DEMO","TableSchema":"TABLE_SCHEMA","Table":"TABLE_'||lpad(statement_id,4,'0')||'"'
                     ||',"rowcount":100000000,"sqlquery":['
                     ||'"CREATE OR REPLACE /* '||lpad(statement_id,4,'0')||' */ '
                     ||' TABLE CONCURRENCY_DEMO.TABLE_SCHEMA.TABLE_'||lpad(statement_id,4,'0')||' AS'
                     ||' SELECT randstr(16,random(11000)+'||statement_id||')::varchar(128) s1 '
                     ||'  ,randstr(16,random(12000)+'||statement_id||')::varchar(128) s2 '
                     ||'  ,randstr(16,random(13000)+'||statement_id||')::varchar(128) s3 '
                     ||' FROM TABLE(generator(rowcount=>100000000))"'
                     ||',"CREATE OR REPLACE /* '||lpad(statement_id,4,'0')||' */ '
                     ||' SECURE VIEW CONCURRENCY_DEMO.TABLE_SCHEMA.VIEW_'||lpad(statement_id,4,'0')||' AS'
                     ||' SELECT * FROM CONCURRENCY_DEMO.TABLE_SCHEMA.TABLE_'||lpad(statement_id,4,'0')||'"'
                     ||',"GRANT  /* '||lpad(statement_id,4,'0')||' */ '
                     ||' SELECT ON VIEW CONCURRENCY_DEMO.TABLE_SCHEMA.VIEW_'||lpad(statement_id,4,'0')
                     ||' TO SHARE CONCURRENCY_DEMO_SHARE "'               
                     ||']'
                     ||'}') task
from  table(generator(rowcount=>100));
```
## Set up
To run the sample code in your environment perform the following steps. It is assumed that the user running the set up and executes the code has the necessary permissions. This could be ACCOUNTADMIN or a custom role that has the following permissions.
    ```
    CREATE WAREHOUSE
    CREATE DATABASE
    EXECUTE TASK
    ```
1. Create a warehouse. The concurrency level is reduced to 2 to ensure that the individual cluster isn't overloaded before Snowflake scales out. The Warehouse_size is set to XSMALL. Your specific use-case may require a bigger size and or more clusters, and or a different MAX_CONCURRENCY_LEVEL. Adjust the parameters accordingly. 
Note: While the framework processes one(1) request, it sets MIN_CLUSTER_COUNT to the number of requested partitions. Doing so ensures that there is one cluster available for each partition.   
    ```
    create warehouse concurrency_demo with
       WAREHOUSE_SIZE = XSMALL
       MAX_CLUSTER_COUNT = 10
       SCALING_POLICY = STANDARD
       AUTO_SUSPEND = 15
       AUTO_RESUME = TRUE
       MAX_CONCURRENCY_LEVEL=2;
    ```
1. Clone this repo (use the command below or any other way to clone the repo)
    ```
    git clone https://github.com/RobertFehrmann/concurrency_framework.git
    ```
1. Create a database and install the stored procedure 
    ```
    create database concurrency_demo;
    create schema concurrency_demo.meta_schema
    ```
1. Create procedure sp_concurrent from the meta_schema directory inside the cloned repo by loading the file into a new worksheet and then clicking `Run`. Note: If you are getting an error message (SQL compilation error: parse ...), move the cursor to the end of the file, click into the window, and then click `Run` again). Be sure to set your context correctly, either from the drop downs in your worksheet or by running the the commands below.
    ```
    use database concurrency_demo;
    use warehouse concurrency_demo;
    use role <ACCOUNTADMIN/your own custom role>
    ```

## Testing

1. Open a new worksheet and set your context, either from the drop downs in your worksheet or by running the commands below.  
    ```
    use database concurrency_demo;
    use warehouse concurrency_demo;
    use role <ACCOUNTADMIN/your own custom role>
    ```
1. Run test for 1 worker thread, 10 tables, 100 million rows per table. This statement should run for about 12 minutes.
    ```
    call meta_schema.sp_concurrent('PROCESS_REQUEST',1,10,'TEST1');
    ```
1. Check table meta_schema.log for the execution log after the call below completes. 
    ```
    select * from meta_schema.log order by id desc;
    ```
1. Run test for 2 worker threads, 10 tables, 100 million rows per table. This statement should run for about 6 minutes, i.e. double the resources for the same amount of work should yield half the run time. 
    ```
    call meta_schema.sp_concurrent('PROCESS_REQUEST',2,5,'TEST2);
    ```
1. Run test for 5 worker thread, 50 tables (!), 100 million rows per table. This statement should run for about 12 minutes, i.e. 5 times the resouces for 5 times the work should yield about the same run time as the first test. This test creates about 1/4 a TB of data and consumes about 1 credit.
    ```
    call meta_schema.sp_concurrent('PROCESS_REQUEST',5,10,'TEST3);
    ```
1. Run test for 10 worker thread, 100 tables (!), 100 million rows per table. This statement should run for about 12 minutes, i.e. 10 times the resources for 10 times the work should yield about the same run time as the first test. This test creates about 1/2 a TB of data and consumes about 2 credits.
    ```
    call meta_schema.sp_concurrent('PROCESS_REQUEST',10,10,'TEST4');
    ```

