create or replace procedure META_SCHEMA.SP_CONCURRENT(I_METHOD VARCHAR,I_METHOD_PARAM_1 float, I_METHOD_PARAM_2 float, I_METHOD_PARAM_3 VARCHAR)
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
const CLUSTER_COUNT=I_METHOD_PARAM_1;
const STEPS_PER_BATCH=I_METHOD_PARAM_2;
const INPUT_TABLE=I_METHOD_PARAM_3;
const WORKER_ID=I_METHOD_PARAM_1;


// keep all constants in a common place
const METHOD_PROCESS_REQUEST='PROCESS_REQUEST';
const METHOD_WORKER='WORKER';

const STATUS_SCHEDULED="SCHEDULED";
const STATUS_ASSIGNED="ASSIGNED";
const STATUS_BEGIN= "BEGIN";
const STATUS_END = "END";
const STATUS_WAIT = "WAIT TO COMPLETE";
const STATUS_WARNING = "WARNING";
const STATUS_FAILURE = "FAILURE";
const STATUS_IDLE = "IDLE";
const TASK_NAME_WORKER="WORKER"

const META_SCHEMA="META_SCHEMA";
const TMP_SCHEMA="TMP_SCHEMA";
const TABLE_NAME="TABLE";
const LOG_TABLE="LOG";
const SCHEDULER_TABLE="SCHEDULER"
const WORK_TABLE="WORK";
const WORKER_TABLE="WORKER";
const TASK_TABLE="TASK";
const TASK_STREAM="STREAM_TASK";

// Worker timeout and polling interval
WORKER_TIMEOUT_COUNT=720;
WAIT_TO_POLL=15;
TASK_TIMEOUT=10800000;

// Global Variables
var current_db="";
var current_warehouse="";
var return_array = [];
var scheduler_session_id=0;
var session_id=0;

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
            var sqlquery = "INSERT INTO " + current_db + "." + META_SCHEMA + "." + LOG_TABLE + " ( method, status,message) values ";
            sqlquery = sqlquery + "('" + METHOD + "','" + status + "','" + message + "');";
            snowflake.execute({sqlText: sqlquery});
            break;
        }
        catch (err) {
            sqlquery=`
                CREATE TABLE IF NOT EXISTS `+ current_db + `.` + META_SCHEMA + `.` + LOG_TABLE + ` (
                    id integer AUTOINCREMENT (0,1)
                    ,create_ts timestamp_tz(9) default current_timestamp
                    ,session_id number default to_number(current_session())
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
        SELECT worker_id
        FROM   "` + current_db + `".` + TMP_SCHEMA + `.` + SCHEDULER_TABLE+ ` 
        WHERE worker_session_id IS NOT NULL
        QUALIFY 1=(row_number() OVER (PARTITION BY worker_id ORDER BY create_ts desc))
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
        SELECT worker_id, worker_session_id,status
        FROM ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE +` s
        WHERE worker_id is not null
        QUALIFY 1=(row_number() OVER (PARTITION BY worker_id ORDER BY create_ts desc))
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
    var batch_id=0;

    log("SET MIN_CLUSTER_COUNT: "+cnt);

    sqlquery=`
        ALTER WAREHOUSE `+current_warehouse+`
        SET MIN_CLUSTER_COUNT=`+cnt+` 
    `;
    snowflake.execute({sqlText:  sqlquery});
}

function assign_next_batch(worker_id) {
    var sqlquery="";
    var batch_id=0;

    log("ASSIGN WORK FOR WORKER: "+worker_id)
    sqlquery=`
        SELECT batch_id
        FROM (  (   SELECT batch_id
                    FROM ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE +`
                    WHERE status = '`+STATUS_SCHEDULED+`'
                    GROUP BY batch_id  
                ) MINUS (
                    SELECT batch_id
                    FROM ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE +`
                    WHERE status in ( '`+STATUS_ASSIGNED+`','`+STATUS_BEGIN+`','`+STATUS_END+`','`+STATUS_FAILURE+`')
                    GROUP BY batch_id
                ))
        ORDER BY batch_id
        LIMIT 1
    `;
    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    if (ResultSet.next()) {
        batch_id=ResultSet.getColumnValue(1);
        log("   BATCH_ID: "+batch_id) 

        for (var i=0;i<2;i++) {
            try {
                sqlquery=`
                    INSERT INTO ` + current_db + `.` + TMP_SCHEMA + `.` + TASK_TABLE+ `_` + worker_id +`
                        VALUES (current_session(),`+batch_id+`,`+worker_id+`)         
                    `;
                snowflake.execute({sqlText:  sqlquery});

                sqlquery=`
                    INSERT INTO ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE+ ` 
                            (scheduler_session_id, batch_id, worker_id, status)
                        VALUES (current_session(), `+batch_id+`,`+worker_id+`,'`+STATUS_ASSIGNED+`')
                `;
                snowflake.execute({sqlText:  sqlquery});
                break;
            }
            catch (err) {
                log("   CREATE TABLE/STREAM/TASK FOR WORKER: "+worker_id)
                sqlquery=`
                    CREATE OR REPLACE TABLE ` + current_db + `.` + TMP_SCHEMA + `.` + TASK_TABLE+ `_` + worker_id +` (
                        scheduler_session_id integer
                        ,batch_id integer
                        ,worker_id integer) 
                `;
                snowflake.execute({sqlText:  sqlquery});
                
                sqlquery=`
                    CREATE OR REPLACE STREAM ` + current_db + `.` + TMP_SCHEMA + `.` + TASK_STREAM + `_` + worker_id +` 
                        ON TABLE ` + current_db + `.` + TMP_SCHEMA + `.` + TASK_TABLE+ `_` + worker_id +` 
                `;
                snowflake.execute({sqlText:  sqlquery});

                sqlquery=`
                    CREATE OR REPLACE TASK ` + current_db + `.` + TMP_SCHEMA + `.` + TASK_NAME_WORKER + `_` + worker_id +`
                        WAREHOUSE =  `+current_warehouse+`
                        USER_TASK_TIMEOUT_MS = `+TASK_TIMEOUT+`
                        SCHEDULE= '1 MINUTE' 
                        WHEN SYSTEM$STREAM_HAS_DATA('` + current_db + `.` + TMP_SCHEMA + `.` + TASK_STREAM + `_` + worker_id +`')
                    AS call `+current_db+`.`+META_SCHEMA+`.`+this_name+`('`+METHOD_WORKER+`',`+worker_id+`,null,null)
                `;
                snowflake.execute({sqlText:  sqlquery});
                
                sqlquery=`
                    ALTER TASK ` + current_db + `.` + TMP_SCHEMA + `.` + TASK_NAME_WORKER + `_` + worker_id + ` resume
                `;
                snowflake.execute({sqlText:  sqlquery});
            }
            assigned=1;
        }   
    } else {
        log("   ALL BATCHES PROCESSED; WORKER IDLE")
        sqlquery=`
            INSERT INTO ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE+ ` 
                    (scheduler_session_id, batch_id, worker_id, status)
                VALUES (current_session(), null,`+worker_id+`,'`+STATUS_IDLE+`')
        `;
        snowflake.execute({sqlText:  sqlquery});

        sqlquery=`
            ALTER TASK ` + current_db + `.` + TMP_SCHEMA + `.` + TASK_NAME_WORKER + `_` + worker_id + ` suspend
        `;
        snowflake.execute({sqlText:  sqlquery});

        assigned=0;
    }

    return assigned;
}

// -----------------------------------------------------------------------------
//  read environment values
// -----------------------------------------------------------------------------
function process_request (cluster_count,steps_per_batch) {
    var sqlquery="";
    var worker_status="";
    var worker_session_id=0;
    var worker_id=0;
    var loop_counter=0;
    var running=0;
    var assigned=0;
    var worker_count=0;
    var batch_count=0;
    var worker_count=0;

    sqlquery=`
        CREATE OR REPLACE SCHEMA `+current_db+`.`+TMP_SCHEMA+`
    `;
    snowflake.execute({sqlText: sqlquery});

    sqlquery=`
        CREATE OR REPLACE TABLE ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE+ ` (
            id integer identity (1,1)
            ,scheduler_session_id integer
            ,worker_session_id integer
            ,worker_id integer
            ,batch_id integer
            ,status varchar
            ,create_ts timestamp_tz(9) default current_timestamp() )
        `;
    snowflake.execute({sqlText: sqlquery});                  

    sqlquery=`
        SELECT 1
        FROM `+current_db+`.information_schema.tables
        WHERE table_schema = '`+META_SCHEMA+`'
          AND table_name = '`+INPUT_TABLE+`'
    `;
    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    if (ResultSet.next()) {
        log("USING INPUT TABLE: "+INPUT_TABLE)
    } else {
        throw new Error("INPUT_TABLE "+ INPUT_TABLE +" IN SCHEMA "+meta_schema+" NOT FOUND")
    }

    //partition work
    sqlquery=`
        CREATE OR REPLACE TABLE ` + current_db + `.` + TMP_SCHEMA + `.` + WORK_TABLE +` AS
            SELECT trunc(seq4()/`+steps_per_batch+`)+1 batch_id, statement_id, task
            FROM ` + current_db + `.` + META_SCHEMA + `.` + INPUT_TABLE +`
        `;
    snowflake.execute({sqlText: sqlquery});
    
    sqlquery=`
        INSERT INTO ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE+ ` 
                (scheduler_session_id,batch_id,status)
            SELECT current_session(), batch_id, '`+STATUS_SCHEDULED+`' 
            FROM ` + current_db + `.` + TMP_SCHEMA + `.` + WORK_TABLE +`
            GROUP BY batch_id
    `;
    snowflake.execute({sqlText: sqlquery});

 
    sqlquery=`
        SELECT count(distinct batch_id)
        FROM ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE+ `
    `;
    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    if (ResultSet.next()) {
        batch_count = ResultSet.getColumnValue(1);
        if ( batch_count > CLUSTER_COUNT) {
            worker_count=CLUSTER_COUNT;
        } else {
            worker_count=batch_count;
        }
    } else {
        throw new Error ("NO WORKER FOUND")
    }

    // pre-allocate one cluster per task (partition) and reset it to 1; This is just to jump-start
    // the creation of all clusters needed.
    if (worker_count>0) {
        set_min_cluster_count(worker_count);
        set_min_cluster_count(1); 
    } 

    //initialize worker array of open worker slots
    worker_queue=[];

    assigned=0;
    for (var i=1; i<=worker_count;i++) {
        assigned+=assign_next_batch(i);
    }
    log("ASSIGNED "+assigned+" BATCHES; CLUSTER COUNT: "+worker_count);

    // when a task starts it puts a record with status BEGIN into the logging table
    // when a task completes it put another record record with status COMPLETE (success) 
    //   or failure into the logging table.

    sqlquery=`
        SELECT worker_id, status
        FROM ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE +` s
        WHERE worker_id is not null
        QUALIFY 1=(row_number() OVER (PARTITION BY worker_id ORDER BY create_ts desc))
        ORDER BY worker_id
    `;
    var completed=0;
    while (true) {
        // Get the status for each scheduled worker
        running=0;
        var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
        while (ResultSet.next()) {
            worker_id        = ResultSet.getColumnValue(1);
            worker_status    = ResultSet.getColumnValue(2);
            if (worker_status==STATUS_FAILURE) {
                log("   WORKER ID "+worker_id+" FAILED" );
                throw new Error("WORKER ID " +worker_id+ " FAILED") 
            } else if (worker_status == STATUS_ASSIGNED) {            
                running+=1;
                log("   WORKER ID "+worker_id+" ASSIGNED");
            } else if (worker_status == STATUS_BEGIN) {
                running+=1;
                log("   WORKER ID "+worker_id+" RUNNING");
            } else if (worker_status == STATUS_END)  {
                log("   WORKER ID "+worker_id+" COMPLETED");
                completed+=1;
                worker_queue.unshift(worker_id)
            } else if (worker_status == STATUS_IDLE)  {
                log("   WORKER ID "+worker_id+" IDLE");
            } else {
                log("UNKNOWN WORKER STATUS "+worker_status)
                throw new Error("UNKNOWN WORKER STATUS "+worker_status+"; ABORT")
            }
        } 

        // assign next batch to workers having completed their work
        assigned=0;
        while(worker_queue.length>0){
            assigned+=assign_next_batch(worker_queue.shift());
        } 

        // break from the loop when all workers have completed 
        //   or wait another interval
        //   or throw an error if the timeout has been exceeded

        if ((running<=0) && (assigned <=0)) {
            log("ALL WORKERS IDLE");
            break;
        } else {
            if (loop_counter<WORKER_TIMEOUT_COUNT) {
                loop_counter+=1;
                log("LOOP: "+loop_counter)
                snowflake.execute({sqlText: "call /* "+loop_counter+" */ system$wait("+WAIT_TO_POLL+")"});
            } else {
                throw new Error("MAX WAIT REACHED; ABORT");
            }
        }                   
    } 

    sqlquery=`
        DROP SCHEMA `+current_db+`.`+TMP_SCHEMA+`
    `;
    //snowflake.execute({sqlText: sqlquery});
}

// read the table parameters (name, # rows) from the configuration in the SCHEDULER table
//   and create the table accordingly

function start_worker (worker_id) {

    var sqlquery="";
    var batch_id=0;
    var statement_id = 0;

    log("GET WORK FOR WORKER: "+worker_id)
    sqlquery=`
        INSERT INTO ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE + `
                (scheduler_session_id, worker_session_id,worker_id, batch_id, status)
            SELECT scheduler_session_id, current_session(),`+worker_id+`,batch_id,'`+STATUS_BEGIN+`'
            FROM ` + current_db + `.` + TMP_SCHEMA + `.` + TASK_STREAM + `_` + worker_id +`
    `;
    snowflake.execute({sqlText: sqlquery});

    sqlquery=`
        SELECT statement_id, task:"sqlquery"
        FROM ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE + ` s
        INNER JOIN ` + current_db + `.` + TMP_SCHEMA + `.` + WORK_TABLE + ` w
            ON w.batch_id=s.batch_id
        WHERE s.worker_session_id=current_session()
    `;

    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    // check if the partition has already been executed by another task
    // if so return with a message, otherwise complete the assigned work
    
    while (ResultSet.next()) {
        statement_id=ResultSet.getColumnValue(1);
        sqlquery = ResultSet.getColumnValue(2);

        log("   EXECUTE STATEMENT: "+statement_id);
        try {
            snowflake.execute({sqlText: sqlquery});
        }
        catch (err) {
            sqlquery=`
                INSERT INTO ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE + `
                    (scheduler_session_id, worker_session_id,worker_id, batch_id, status)
                    SELECT scheduler_session_id, worker_session_id,worker_id,batch_id,'`+STATUS_FAILURE+`'
                    FROM ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE + `
                    WHERE worker_session_id=current_session()
            `;
            snowflake.execute({sqlText: sqlquery});
            throw Error(err);
        }
    }

    sqlquery=`
        INSERT INTO ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE + `
                (scheduler_session_id, worker_session_id,worker_id, batch_id, status)
            SELECT scheduler_session_id, worker_session_id,worker_id,batch_id,'`+STATUS_END+`'
            FROM ` + current_db + `.` + TMP_SCHEMA + `.` + SCHEDULER_TABLE + `
            WHERE worker_session_id=current_session()
    `;
    snowflake.execute({sqlText: sqlquery});
}

try {

    init();

    log("procName: " + procName + " " + STATUS_BEGIN);
    flush_log(STATUS_BEGIN);

    if (METHOD==METHOD_PROCESS_REQUEST) {
        worker_id=0;
        process_request(CLUSTER_COUNT,STEPS_PER_BATCH);
    } else if (METHOD==METHOD_WORKER){
        start_worker(WORKER_ID)
    } else {
        throw new Error("REQUESTED METHOD NOT FOUND; "+METHOD);
    }

    log("procName: " + procName + " " + STATUS_END);
    flush_log(STATUS_END);
    return return_array; 
}
catch (err) {
    log("ERROR found - MAIN try command");
    log("err.code: " + err.code);
    log("err.state: " + err.state);
    log("err.message: " + err.message);
    flush_log(STATUS_FAILURE);

    suspend_all_workers();
    kill_all_running_worker_queries();

    return return_array;
}
$$;