create or replace procedure META_SCHEMA.SP_CONCURRENT(I_METHOD VARCHAR,I_METHOD_PARAM_1 float, I_METHOD_PARAM_2 float, I_METHOD_PARAM_3 float)
    returns ARRAY
    language JAVASCRIPT
    execute as caller
as
$$
// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-09-30
// Purpose     SP_CONCURRENT is a "library" of functions to perform the work for a embarrassingly parallel
//             problem. 
//             The stored procedure accepts the following methods:
//                PROCESS_REQUEST: 
//                   Parameters:
//                       METHOD_PARAM_1: #of partitions to create. Each partition will be executed by one worker
//                       METHOD_PARAM_2: #of tables to create 
//                       METHOD_PARAM_3: #of rows per table
//                   This method is the scheduler (coordinator) method. It divides the total number of tables into 
//                   equal partitions (number of tables) per worker and stores instructions in the SCHEDULER table.
//                   Then it creates one task(worker) per partition 
//                   and waits until all workers complete or a maximum wait time has passed.
//                WORKER: The worker method performs the actual work. It creates the tables based on the configuration
//                   in the SCHEDULER table.
//             Process coordination is accomplished via the SCHEDULER and the LOG tables.
//
//             To show the framework in action the sample code here shows how to create and generate data for a set
//             of tables by creating a configurable number of parallel tasks
// -----------------------------------------------------------------------------
// Modification History
//
// 2020-09-30 Robert Fehrmann  
//      Initial Version
// -----------------------------------------------------------------------------
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------


// copy parameters into local constant; none of the input values can be modified
const METHOD= I_METHOD;
const PARTITION_COUNT=I_METHOD_PARAM_1;
const TABLE_COUNT=I_METHOD_PARAM_2;
const ROW_COUNT=I_METHOD_PARAM_3;
const WORKER_ID=I_METHOD_PARAM_1;

// keep all constants in a common place
const METHOD_PROCESS_REQUEST='PROCESS_REQUEST';
const METHOD_WORKER='WORKER';

const STATUS_BEGIN= "BEGIN";
const STATUS_END = "END";
const STATUS_WAIT = "WAIT TO COMPLETE";
const STATUS_WARNING = "WARNING";
const STATUS_FAILURE = "FAILURE";
const TASK_NAME_WORKER="WORKER"

const META_SCHEMA="META_SCHEMA";
const TMP_SCHEMA="TMP_SCHEMA";
const TABLE_SCHEMA="TABLE_SCHEMA";
const TABLE_NAME="TABLE";
const LOG_TABLE="LOG";
const SCHEDULER_TABLE="SCHEDULER";

// Worker timeout and polling interval
WORKER_TIMEOUT_COUNT=360;
WAIT_TO_POLL=30;
TASK_TIMEOUT=10800000;



// Global Variables
var current_db="";
var current_warehouse="";
var return_array = [];
var scheduler_session_id=0;
var session_id=0;
var partition_id=0;

var this_name = Object.keys(this)[0];
var procName = this_name + "-" + METHOD;


// -----------------------------------------------------------------------------
//  log a debug message in the results array
// -----------------------------------------------------------------------------
function log ( msg ) {
    var d=new Date();
    var UTCTimeString=("00"+d.getUTCHours()).slice(-2)+":"+("00"+d.getUTCMinutes()).slice(-2)+":"+("00"+d.getUTCSeconds()).slice(-2);
    return_array.push(UTCTimeString+" "+msg);
}

// -----------------------------------------------------------------------------
//  persist all debug messages in the results array into one row in the log table 
// -----------------------------------------------------------------------------
function flush_log (status){
    var message="";
    var sqlquery="";

    for (i=0; i < return_array.length; i++) {
        message=message+String.fromCharCode(13)+return_array[i];
    }
    message=message.replace(/'/g,""); //' keep formatting in VS nice

    for (i=0; i<2; i++) {
        try {
            var sqlquery = "INSERT INTO " + current_db + "." + META_SCHEMA + "." + LOG_TABLE + " ( scheduler_session_id, partition_id, method, status,message) values ";
            sqlquery = sqlquery + "("+scheduler_session_id +","+partition_id+ ",'" + METHOD + "','" + status + "','" + message + "');";
            snowflake.execute({sqlText: sqlquery});
            break;
        }
        catch (err) {
            sqlquery=`
                CREATE TABLE IF NOT EXISTS `+ current_db + `.` + META_SCHEMA + `.` + LOG_TABLE + ` (
                    id integer AUTOINCREMENT (0,1)
                    ,create_ts timestamp_tz(9) default current_timestamp
                    ,scheduler_session_id number default 0
                    ,session_id number default to_number(current_session())
                    ,partition_id integer
                    ,method varchar
                    ,status varchar
                    ,message varchar)`;
            snowflake.execute({sqlText: sqlquery});
        }
    }
}

// -----------------------------------------------------------------------------
//  read environment values
// -----------------------------------------------------------------------------
function init () {
    var sqlquery="";

    sqlquery=`
        SELECT current_warehouse(),current_database(),current_session()
    `;
    snowflake.execute({sqlText: sqlquery});

    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    if (ResultSet.next()) {
        current_warehouse=ResultSet.getColumnValue(1);
        current_db=ResultSet.getColumnValue(2);
        session_id=ResultSet.getColumnValue(3);
    } else {
        throw new Error ("INIT FAILURE");
    }
}

// -----------------------------------------------------------------------------
//  cleanup; suspend all tasks after failure 
// -----------------------------------------------------------------------------
function suspend_all_workers () {

    var worker_id=0;

    log("   SUSPEND ALL WORKERS");
    var sqlquery=`
        SELECT partition_id
        FROM   "` + current_db + `".` + TMP_SCHEMA + `.` + SCHEDULER_TABLE+ ` 
        ORDER BY 1
    `;

    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    while (ResultSet.next()) {
        worker_id=ResultSet.getColumnValue(1); 

        log("      SUSPEND WORKER "+TASK_NAME_WORKER+"_"+worker_id);

        sqlquery=`
            ALTER TASK `+current_db+`.`+TMP_SCHEMA+`.`+TASK_NAME_WORKER+`_`+worker_id+` suspend`;
        snowflake.execute({sqlText: sqlquery});
    }
}

// -----------------------------------------------------------------------------
//  cleanup; kill all running queries after failure 
// -----------------------------------------------------------------------------
function kill_all_running_worker_queries() {
    var sqlquery="";
    var sqlquery2="";
    var sqlquery3="";
    var worker_id=0;
    var worker_session_id=0;
    var query_id="";

    log("   CANCEL RUNNING QUERIES")

    sqlquery=`
        WITH worker_log AS (
            SELECT partition_id, status, scheduler_session_id, session_id, create_ts
            FROM ` + current_db + `.` + META_SCHEMA + `.` + LOG_TABLE + `
            WHERE status = '`+STATUS_BEGIN+`'
                AND method = '`+METHOD_WORKER+`'
            QUALIFY 1=(row_number() OVER (PARTITION BY scheduler_session_id,partition_id ORDER BY create_ts desc))
        )
        SELECT s.partition_id,l.session_id worker_session_id
        FROM  ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE + `  s
            INNER JOIN worker_log l
            ON l.scheduler_session_id = s.scheduler_session_id 
               AND l.partition_id=s.partition_id
               AND l.create_ts > s.create_ts
        WHERE s.scheduler_session_id=current_session()
        ORDER BY s.partition_id
    `;
    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
    while (ResultSet.next()) {
        worker_id        = ResultSet.getColumnValue(1);
        worker_session_id= ResultSet.getColumnValue(2);

        log("      FIND RUNNING QUERIES FOR WORKER ID: "+worker_id+" SESSION_ID: "+worker_session_id);

        sqlquery2=`
            SELECT query_id 
            FROM table(information_schema.query_history_by_session(SESSION_ID=>`+worker_session_id+`,RESULT_LIMIT=>1000))
            WHERE execution_status='RUNNING'
            ORDER BY start_time DESC;
        `;
        var ResultSet2 = (snowflake.createStatement({sqlText:sqlquery2})).execute();
        while (ResultSet2.next()) {
            query_id = ResultSet2.getColumnValue(1);

            log("         CANCEL QUERY: "+query_id);

            sqlquery3=`
                SELECT SYSTEM$CANCEL_QUERY('`+query_id+`')
            `;
            snowflake.execute({sqlText:  sqlquery3});
        }
    }
}

// -----------------------------------------------------------------------------
//  pre-allocate compute tier
// -----------------------------------------------------------------------------
function set_min_cluster_count(cnt) {
    var sqlquery="";

    log("   SET MIN_CLUSTER_COUNT: "+cnt);

    sqlquery=`
        ALTER WAREHOUSE `+current_warehouse+`
        SET MIN_CLUSTER_COUNT=`+cnt+` 
    `;
    snowflake.execute({sqlText:  sqlquery});
}

// -----------------------------------------------------------------------------
//  read environment values
// -----------------------------------------------------------------------------
function process_request (partition_count,table_count,table_name,row_count) {
    var sqlquery="";
    var worker_status="";
    var worker_session_id=0;
    var worker_id=0;
    var loop_counter=0;
    var counter=0;

    log("procName: " + procName + " " + STATUS_BEGIN);
    flush_log(STATUS_BEGIN);

    sqlquery=`
        CREATE OR REPLACE SCHEMA `+current_db+`.`+TMP_SCHEMA+`
    `;
    snowflake.execute({sqlText: sqlquery});

    sqlquery=`
        CREATE OR REPLACE SCHEMA `+current_db+`.`+TABLE_SCHEMA+`
    `;
    snowflake.execute({sqlText: sqlquery});

    // generate one row per table to be created and assign each to a partition
    //   store the min and max values for each partition along with the 
    //   partition id, table_name, and row_count per table in the SCHEDULER_TABLE
    sqlquery=`
        CREATE OR REPLACE TABLE ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE+ ` AS
            SELECT current_session() scheduler_session_id,partition_id, '`+table_name+`' table_name
                   , min(id)+1 min_table_id, max(id)+1 max_table_id,`+ROW_COUNT+` row_count
                   , current_timestamp()::timestamp_tz(9) create_ts
            FROM (
                SELECT seq4() id, trunc(id*(`+partition_count+`/`+table_count+`)+1) partition_id
                FROM   table(generator(rowcount=>`+table_count+`))
                ) 
            GROUP BY partition_id
            ORDER BY partition_id
    `;
    snowflake.execute({sqlText:  sqlquery});

    // create a task for each partition (partition ids are assumed to be unique)
    sqlquery=`
        SELECT partition_id
        FROM   "` + current_db + `"."` + TMP_SCHEMA + `"."` + SCHEDULER_TABLE+ `" 
        ORDER BY 1
    `;

    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    while (ResultSet.next()) {
        worker_id=ResultSet.getColumnValue(1); 
        sqlquery=`
            CREATE OR REPLACE TASK ` + current_db + `.` + TMP_SCHEMA + `.` + TASK_NAME_WORKER + `_` + worker_id +`
                WAREHOUSE =  `+current_warehouse+`
                USER_TASK_TIMEOUT_MS = `+TASK_TIMEOUT+`
                SCHEDULE= '1 MINUTE' 
            AS call `+current_db+`.`+META_SCHEMA+`.`+this_name+`('`+METHOD_WORKER+`',`+worker_id+`,null,null)
        `;
        snowflake.execute({sqlText:  sqlquery});
      
        sqlquery=`
            ALTER TASK ` + current_db + `.` + TMP_SCHEMA + `.` + TASK_NAME_WORKER + `_` + worker_id + ` resume
        `;
        snowflake.execute({sqlText:  sqlquery});
    }

    // pre-allocate one cluster per task (partition) and reset it to 1; This is just to jump-start
    // the creation of all clusters needed.
    set_min_cluster_count(partition_count);
    set_min_cluster_count(1);   
           
    // when a task starts it puts a record with status BEGIN into the logging table
    // when a task completes it put another record record with status COMPLETE (success) 
    //   or failure into the logging table.

    sqlquery=`
        WITH worker_log AS (
            SELECT partition_id, status, scheduler_session_id, session_id, create_ts
            FROM ` + current_db + `.` + META_SCHEMA + `.` + LOG_TABLE + `
            WHERE status in ('`+STATUS_BEGIN+`','`+STATUS_END+`','`+STATUS_FAILURE+`')
                AND method = '`+METHOD_WORKER+`'
            QUALIFY 1=(row_number() OVER (PARTITION BY scheduler_session_id,partition_id ORDER BY create_ts desc))
        )
        SELECT s.partition_id, nvl(l.session_id,0) worker_session_id, nvl(l.status,'`+STATUS_WAIT+`')
        FROM  ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE + `  s
            LEFT OUTER JOIN worker_log l
            ON l.scheduler_session_id = s.scheduler_session_id 
               AND l.partition_id=s.partition_id
               AND l.create_ts > s.create_ts
        WHERE s.scheduler_session_id=current_session()
        ORDER BY s.partition_id
    `;

    loop_counter=0
    while (true) {

        counter=0;

        // Get the status for each scheduled task (worker)

        var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
        while (ResultSet.next()) {
            worker_id        = ResultSet.getColumnValue(1);
            worker_session_id= ResultSet.getColumnValue(2);
            worker_status    = ResultSet.getColumnValue(3);
            if (worker_status==STATUS_FAILURE) {
                log("   WORKER "+worker_id+" FAILED" );
                throw new Error("WORKER ID " +worker_id+ " FAILED") 
            } else if (worker_status == STATUS_WAIT) {            
                counter+=1;
                log("   WAITING FOR WORKER "+worker_id+" TO START");
            } else if (worker_status == STATUS_BEGIN) {
                counter+=1;
                log("   WORKER ID "+worker_id+" RUNNING");
            } else if (worker_status == STATUS_END)  {
                log("   WORKER ID "+worker_id+" COMPLETED");
            } else {
                log("UNKNOWN WORKER STATUS "+worker_status)
                throw new Error("UNKNOWN WORKER STATUS "+worker_status+"; ABORT")
            }
        }

        // break from the loop when all workers have completed 
        //   or wait another interval
        //   or throw an error if the timeout has been exceeded

        if (counter<=0)  {
            log("ALL WORKERS COMPLETED SUCCESSFULLY");
            break;
        } else {
            if (loop_counter<WORKER_TIMEOUT_COUNT) {
                loop_counter+=1;
                log("   WAITING FOR "+counter+" WORKERS TO COMPLETE; LOOP CNT "+loop_counter);
                snowflake.execute({sqlText: "call /* "+loop_counter+" */ system$wait("+WAIT_TO_POLL+")"});
            } else {
                throw new Error("MAX WAIT REACHED; ABORT");
            }
        }                   
    } 

    sqlquery=`
        DROP SCHEMA `+current_db+`.`+TMP_SCHEMA+`
    `;
    snowflake.execute({sqlText: sqlquery});

    log("procName: " + procName + " " + STATUS_END);
    flush_log(STATUS_END);
}

// read the table parameter from the configuration in the SCHEDULER table
//   and create the table accordingly

function process_work(partition_id) {
    var sqlquery="";
    var table_name=""
    var row_count=0;
    var min_table_id=0;
    var max_table_id=0;

    sqlquery=`
        SELECT table_name, row_count, min_table_id, max_table_id
        FROM ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE +`
        WHERE partition_id = `+ partition_id +`
    `;

    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    if (ResultSet.next()) {
        table_name=ResultSet.getColumnValue(1);
        row_count=ResultSet.getColumnValue(2);
        min_table_id=ResultSet.getColumnValue(3);
        max_table_id=ResultSet.getColumnValue(4);
    } else {
        throw new Error ("ERROR IN PARTITION "+partition_id);
    }

    for (i=min_table_id;i<=max_table_id;i++) {
        log("   CREATE TABLE_"+i);
        sqlquery=`
            CREATE OR REPLACE /* `+("0000"+i.toString()).slice(-4)+` */ 
                TABLE `+current_db+`.`+TABLE_SCHEMA+`.`+table_name+`_`+("0000"+i.toString()).slice(-4)+` AS
                SELECT 
                    randstr(16,random(11000)+`+partition_id+`)::varchar(128) s1
                    ,randstr(16,random(12000)+`+partition_id+`)::varchar(128) s2
                    ,randstr(16,random(13000)+`+partition_id+`)::varchar(128) s3
                FROM TABLE(generator(rowcount=>`+row_count+`));
        `; 
        snowflake.execute({sqlText: sqlquery});
    }
}

// read the table parameters (name, # rows) from the configuration in the SCHEDULER table
//   and create the table accordingly

function start_worker (worker_id) {
    const PARTITION_ID=worker_id;

    var sqlquery="";
    var worker_session_id=0;

    // suspend the task to avoid it from being started again
    sqlquery=`
        ALTER TASK `+current_db+`.`+TMP_SCHEMA+`.`+TASK_NAME_WORKER+`_`+WORKER_ID+` suspend`;
    snowflake.execute({sqlText: sqlquery});

    sqlquery=`
        WITH worker_log 
        AS (
            SELECT partition_id, status, scheduler_session_id, session_id, create_ts
            FROM ` + current_db + `.` + META_SCHEMA + `.` + LOG_TABLE + `
            WHERE status in ('`+STATUS_BEGIN+`','`+STATUS_END+`','`+STATUS_FAILURE+`')
                AND method = '`+METHOD_WORKER+`'
            QUALIFY 1=(row_number() OVER (PARTITION BY scheduler_session_id,partition_id ORDER BY create_ts desc))
        )           
        SELECT s.scheduler_session_id,s.partition_id, nvl(l.session_id,0) worker_session_id
        FROM ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE +` s
        LEFT OUTER JOIN worker_log l 
            ON l.scheduler_session_id = s.scheduler_session_id AND l.partition_id=s.partition_id AND l.create_ts > s.create_ts
        WHERE s.partition_id = `+PARTITION_ID+`
    `;

    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    // check if the partition has already been executed by another task
    // if so return with a message, otherwise complete the assigned work
    
    if(ResultSet.next()){
        scheduler_session_id = ResultSet.getColumnValue(1);
        worker_session_id=ResultSet.getColumnValue(3)

        log("procName: " + procName + " " + STATUS_BEGIN);
        flush_log(STATUS_BEGIN);

        if (worker_session_id == 0) {
            log("PROCESSING TASKS FOR SCHEDULER ID "+scheduler_session_id+" PARTITION "+PARTITION_ID);
            process_work(PARTITION_ID);
        } else {
            log("PROCESSING FOR SCHEDULER ID "+scheduler_session_id+" PARTITION "+PARTITION_ID+" ALREADY DONE BY SESSION "+worker_session_id);
        }

        log("procName: " + procName + " " + STATUS_END);
        flush_log(STATUS_END);
        return return_array;
    } else {
        throw new Error ("PARTITION NOT FOUND")
    }
}

try {
    init();
    if (METHOD==METHOD_PROCESS_REQUEST) {
        process_request(PARTITION_COUNT,TABLE_COUNT,TABLE_NAME,ROW_COUNT);
    } else if (METHOD==METHOD_WORKER){
        partition_id=WORKER_ID;
        start_worker(WORKER_ID)
    } else {
        throw new Error("REQUESTED METHOD NOT FOUND; "+METHOD);
    }
    return return_array;
}
catch (err) {
    log("ERROR found - MAIN try command");
    log("err.code: " + err.code);
    log("err.state: " + err.state);
    log("err.message: " + err.message);

    try {
        suspend_all_workers();
        kill_all_running_worker_queries();
    }
    catch(err) {
        log("err.code: " + err.code);
        log("err.state: " + err.state);
        log("err.message: " + err.message);
    }

    flush_log(STATUS_FAILURE);
    return return_array;
}
$$;
