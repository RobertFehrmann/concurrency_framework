use database concurrency_demo;
create or replace schema concurrency_demo.roles;

show grants to role PUBLIC;
create or replace table roles.roles_snapshot 
  as  select * from table(result_scan(last_query_id())) where 1=0 ;

// the following two statements need to be executed together
show roles;
create or replace table meta_schema.roles_demo as 
select 
seq4() statement_id
,parse_json ('{"Role":"'||"name"||'"'
                     ||',"sqlquery":['
                     ||'"SHOW GRANTS TO ROLE '||"name"||'"'
                     ||',"INSERT INTO concurrency_demo.roles.roles_snapshot SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"'
                     ||']'
                     ||'}') task
from  table(result_scan(last_query_id()));

// play with the parameters to evalute the impact on run time
// 5 workers, one statement block per batch
call meta_schema.sp_concurrent('PROCESS_REQUEST',1,5,'ROLES_DEMO');

// 5 workers, one statement block per batch
call meta_schema.sp_concurrent('PROCESS_REQUEST',5,1,'ROLES_DEMO');