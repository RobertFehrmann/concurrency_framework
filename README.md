# concurrency_framework
Framework for processing work concurrently via Snowflake JS stored proc 


[Snowflake stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures-overview.html) use JavaScript to express complex logic. The sample code here shows how a single stored procedure can initiate concurrent code execution by using [Snowflake Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro.html). 
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
1. Clone this repo (use the command below or any other way to clone the repo)
    ```
    create warehouse concurrent_js_test with
       WAREHOUSE_SIZE = XSMALL
       MAX_CLUSTER_COUNT = 10
       SCALING POLICY = STANDARD
       AUTO_SUSPEND = 15
       AUTO_RESUME = TRUE
       MAX_CONCURRENCY_LEVEL=4;
    ```
1. Clone this repo (use the command below or any other way to clone the repo)
    ```
    git clone https://github.com/RobertFehrmann/concurrent_js_sample.git
    ```
1. Create a database and install the stored procedure 
    ```
    create database concurrent_js_test;
    create schema concurrent_js_test.meta_schema
    ```
1. Create procedure sp_concurrent_js from the meta_schema directory inside the cloned repo by loading the file into a new worksheet and then clicking `Run`. Note: If you are getting an error message (SQL compilation error: parse ...), move the cursor to the end of the file, click into the window, and then click `Run` again). Be sure to set your context correctly, either from the drop downs in your worksheet or by running the the commands below.
    ```
    use database concurrent_js_test;
    use warehouse concurrent_js_test;
    use role <ACCOUNTADMIN/your own custom role>
    ```

## Testing
1. Open a new worksheet and set your context. Be sure to set your context correctly, either from the drop downs in your worksheet or by running the the commands below.
    ```
    use database concurrent_js_test;
    use warehouse concurrent_js_test;
    use role <ACCOUNTADMIN/your own custom role>
    ```
1. Run test for 1 worker thread, 100 tables, 1 million rows per table. This statement should run for about 7:30 minutes.
    ```
    call meta_schema.sp_concurrent_js('PROCESS_REQUEST',1,10,1000000);
    ```
1. Check table meta_schema.log for the execution log after the call below completes. 
    ```
    select * from meta_schema.log order by id desc;
    ```
1. Run test for 10 worker thread, 10 tables, 1 million rows per table. This statement should run for about 2:30 minutes. We are not seeing 
    ```
    call meta_schema.sp_concurrent_js('PROCESS_REQUEST',1,10,1000000);
    ```
