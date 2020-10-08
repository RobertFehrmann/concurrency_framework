# concurrency_framework
Framework for processing work concurrently via Snowflake JS stored proc 


[Snowflake stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures-overview.html) use JavaScript to express complex logic. As of version 4.32.1, Snowflake stored procedures do not support a threading model, i.e. Snowflake stored procedures execute single threaded. How, that doesn't mean, we couldn't execute a workfload in parallel. The sample code here shows how a single stored procedure can initiate concurrent code execution by using [Snowflake Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro.html). The idea is to partition the total amount of work into N partitions. Each partition of work will be handled by 1 task. Multiple tasks can execute on a single cluster. Snowflake will automatically scale out when queuing occurs after MAX_CONCURRENCY_LEVEL (see below) is reached. 

The sample code takes 4 input parameters.
* method name ('PROCESS_REQUEST') 
* number of partitions
* number of tables to be created
* number of rows to be created per table

It implements a simple model for an [Embarrassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel) problem. To adapt the sample code for you own processing, you have to 
* define how to partition the total amount of work into N partitions of approx the same amount of work (measured in execution time)
* define how to execute the work for each partition

## Set up
To run the sample code in your environment perform the following steps. It is assumed that the user running the set up and executes the code has the necessary permissions. This could be ACCOUNTADMIN or a custom role that has the following permissions.
    ```
    CREATE WAREHOUSE
    CREATE DATABASE
    EXECUTE TASK
    ```
1. Create a warehouse. The concurrency level is reduced to 4 to ensure that the individual cluster isn't overloaded before Snowflake scales out. The Warehouse_size is set to XSMALL. Your specific use-case may require a bigger size and or more clusters. Adjust the parameters accordingly. 
    ```
    create warehouse concurrency_test with
       WAREHOUSE_SIZE = XSMALL
       MAX_CLUSTER_COUNT = 20
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
    create database concurrency_test;
    create schema concurrency_test.meta_schema
    ```
1. Create procedure sp_concurrent_js from the meta_schema directory inside the cloned repo by loading the file into a new worksheet and then clicking `Run`. Note: If you are getting an error message (SQL compilation error: parse ...), move the cursor to the end of the file, click into the window, and then click `Run` again). Be sure to set your context correctly, either from the drop downs in your worksheet or by running the the commands below.
    ```
    use database concurrency_test;
    use warehouse concurrency_test;
    use role <ACCOUNTADMIN/your own custom role>
    ```

## Testing

1. Open a new worksheet and set your context. Be sure to set your context correctly, either from the drop downs in your worksheet or by running the the commands below.
    ```
    use database concurrency_test;
    use warehouse concurrency_test;
    use role <ACCOUNTADMIN/your own custom role>
    ```
1. Run test for 1 worker thread, 10 tables, 100 million rows per table. This statement should run for about 10 minutes.
    ```
    call meta_schema.sp_concurrent('PROCESS_REQUEST',1,10,100000000);
    ```
1. Check table meta_schema.log for the execution log after the call below completes. 
    ```
    select * from meta_schema.log order by id desc;
    ```
1. Run test for 2 worker threads, 10 tables, 100 million rows per table. This statement should run for about 6 minutes. 
    ```
    call meta_schema.sp_concurrent('PROCESS_REQUEST',2,10,100000000);
    ```
1. Run test for 10 worker thread, 100 tables (!), 100 million rows per table. This statement should run for about 10 minutes. This test creates about 1/2 a TB of data and consumes about 1.5 credits.
    ```
    call meta_schema.sp_concurrent('PROCESS_REQUEST',10,100,100000000);
    ```
1. Run test for 20 worker thread, 100 tables (!), 100 million rows per table. This statement should run for about 6 minutes. This test creates about 1/2 a TB of data and consumes about 2 credits.
    ```
    call meta_schema.sp_concurrent('PROCESS_REQUEST',20,100,100000000);
    ```
