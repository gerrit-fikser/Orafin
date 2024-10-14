DECLARE
    l_count NUMBER;
BEGIN
    
    select count(1)
    into l_count
    from ALL_SCHEDULER_JOBS
    where job_name = 'KDA_OUR_DWH_FA_SUBLEDGER_EXTRACT';

    if l_count > 0 then  
        DBMS_SCHEDULER.drop_job(job_name => 'KDA_OUR_DWH_FA_SUBLEDGER_EXTRACT');
    end if;

    DBMS_SCHEDULER.create_job(
      job_name              => 'KDA_OUR_DWH_FA_SUBLEDGER_EXTRACT',
      job_type              => 'STORED_PROCEDURE',
      job_action            => 'DWH_EXTRACT_PKG.EXECUTE_EXTRACT_PROCESS',
      number_of_arguments   => 1,
      start_date            => SYSTIMESTAMP,
      repeat_interval       => 'FREQ=HOURLY;INTERVAL=1;BYMINUTE=5', 
      enabled               => FALSE
   );

    DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE (
        job_name           => 'KDA_OUR_DWH_FA_SUBLEDGER_EXTRACT',
        argument_position  => 1,
        argument_value     => 'DWH_FA_SUBLEDGER');
END;
/