create or replace package body "DWH_EXTRACT_PKG" as
--*****************************************************************************
--Module      : ERP Outbound Reporting
--Type        : PL/SQL - Package
--Author      : Aldis Lagzdins
--Version     : 1.0
--
--
-- Description: Invoke extract BI reports and upload ready files to bucket for DataWarehouse further processing 
--
-- *****************************************************************************

-- -------------------------------------------------
-- Change log
-- Date        Author          Version     Comment
----------------------------------------------------
-- 17.09.2024  Aldis Lagzdins  1.0         Created
-- -------------------------------------------------

/*===========================================
============= Global Variables ==============
===========================================*/

g_pkg           constant varchar2(20)  := 'DWH_EXTRACT_PKG';
g_proc                   varchar2(50)  := '';
g_step                   varchar2(100) := '';
g_module        constant varchar2(3)   := 'DWH';

/*===========================================
==== PRIVATE Procedures and Functions =======
===========================================*/

--Log error
PROCEDURE log_error(
    p_msg_id  in dbo_msg_outbound.msg_id%type,
    p_message in varchar2
) IS 
    l_msg_pref varchar2(200);
BEGIN
    l_msg_pref := g_pkg||'.'||g_proc||'>>'||g_step||':';
    if p_msg_id is not null then --does not support error loging without MSG_ID
        dbo_msg_pkg.log_error(p_msg_id, substr(l_msg_pref||p_message,1,2000), 'ERROR');
        dbo_msg_pkg.debug_msg(p_msg_id,g_module,substr(l_msg_pref||p_message,1,2000));
    end if;
END log_error;

--debug
PROCEDURE debug(
    p_msg_id  in dbo_msg_outbound.msg_id%type,
    p_message in varchar2
) IS 
    l_msg_pref varchar2(200);
BEGIN
    l_msg_pref := g_pkg||'.'||g_proc||'>>'||g_step||':';    
    dbo_msg_pkg.debug_msg(p_msg_id,g_module,substr(l_msg_pref||p_message,1,2000));
END debug;

PROCEDURE remove_header_row (
    p_msg_id IN raw,
    p_blob IN OUT blob,
    p_length OUT number
) IS
    l_dest_clob CLOB;
    l_src_clob CLOB;
    l_start_pos NUMBER;
    l_length_to_copy NUMBER;
BEGIN
    
    l_src_clob := apex_util.blob_to_clob(p_blob => p_blob);
    
    --get length of clob
    p_length := DBMS_LOB.GETLENGTH(l_src_clob);

    -- Find the position of first new line
    l_start_pos := DBMS_LOB.INSTR(l_src_clob, CHR(10), 1);

    IF l_start_pos is not null and l_start_pos > 0 THEN
        
        -- Calculate the length to be copied when excluding header row
        IF p_length is not null THEN
            l_length_to_copy := p_length - l_start_pos;
            
            DBMS_LOB.CREATETEMPORARY(l_dest_clob, TRUE);
            
            --copy clob all content except header row
            DBMS_LOB.COPY ( 
                  dest_lob    => l_dest_clob,
                  src_lob     => l_src_clob,
                  amount      => l_length_to_copy,
                  dest_offset => 1,
                  src_offset  => l_start_pos + 1);
                            
            p_blob := apex_util.clob_to_blob(p_clob => l_dest_clob);

            DBMS_LOB.FREETEMPORARY(l_dest_clob);
        END IF;
    END IF;
    debug(p_msg_id, 'l_start_pos='||l_start_pos||' l_length_to_copy='||l_length_to_copy);
END remove_header_row;

/*===========================================
==== PUBLIC Procedures and Functions =======
===========================================*/

/*
--function to check if extract name is valid. Inwoked from WF.
function get_extract_id (
    p_msg_id       in dbo_msg_outbound.msg_id%type,
    p_extract_name in dwh_extract_setup.name%type
) return number
is 
    l_id dwh_extract_setup.id%type;
begin 
    g_proc := 'get_extract_id';
    select id 
    into l_id
    from dwh_extract_setup 
    where name = p_extract_name
    and enabled = 'Y';

    return l_id;
exception 
    when others then 
        log_error(p_msg_id, SQLERRM);
        raise;
end get_extract_id;
*/

--create and store outbound message. start apex automation. invoked from DWH setup screen
procedure start_adhoc_extract(
    p_extract_id    IN  dwh_extract_setup.id%type,
    p_param_json    IN  clob,
    p_msg_id        OUT dbo_msg_outbound.msg_id%type
) IS

l_name             dwh_extract_setup.name%type;
l_autom_static_id  dwh_extract_setup.automation_static_id%type;
l_filters          apex_exec.t_filters;
l_context          apex_exec.t_context;

begin
    g_proc := 'start_adhoc_extract';

    p_msg_id := sys_guid();

    select 'MANUAL '||name, automation_static_id
    into l_name, l_autom_static_id
    from dwh_extract_setup
    where id = p_extract_id;

    g_step := 'Store outbound message';
    dbo_msg_pkg.store_outbound_msg(
        p_msg_id   => p_msg_id,
        p_payload   => p_param_json,
        p_msg_type  => g_module,
        p_source_ref => l_name
    );

    g_step := 'Execute automation';
    if p_msg_id is not null then
        BEGIN

/*
            apex_exec.add_filter(
                p_filters        => l_filters,
                p_column_name    => 'MSG_ID',
                p_filter_type    => apex_exec.c_filter_eq,
                p_value          => p_msg_id );

            apex_automation.execute(
                p_static_id       => 'dwh-ad-hoc-extracts',
                p_filters         => l_filters );
*/
            apex_automation.execute(
                p_static_id         => 'dwh-ad-hoc-extracts',
                p_run_in_background => true
            );
                        
        
        EXCEPTION
            WHEN OTHERS THEN
                log_error(p_msg_id, 'Unexpected error: ' || SQLERRM);
                dbo_msg_pkg.update_outbound_status(p_msg_id,'ERROR');
                raise;
        END;
    end if;

end start_adhoc_extract;

--run extract invoked from DWH setup screen
procedure run_extract(
    p_msg_id        IN dbo_msg_outbound.msg_id%type,
    p_extract_id    IN dwh_extract_setup.id%type,
    p_parameters    IN dwh_extract_parameter_tab,
    p_result        OUT VARCHAR2, 
    p_error_message OUT VARCHAR2
) IS  
    l_clob            CLOB;
	l_clob_in         CLOB;
    l_blob            BLOB;
    l_temp_blob       BLOB;
    l_zip_file        BLOB;
    l_xml             XMLTYPE;
    l_bi_report       dwh_extract_setup.bi_report%type;
    l_obj_path        dwh_extract_setup.oci_bucket_path%type;
    l_obj_storage_url con_etlsettings.objectstorageurl%type;
    l_file_name       dwh_extract_setup.file_name%type;
    l_file_type       dwh_extract_setup.file_type%type;
    l_compress_flag   dwh_extract_setup.compress_flag%type;
    l_file_ext        VARCHAR2(10);
    l_credentials     VARCHAR2(255);
    l_result          VARCHAR2(50);
    l_ledger_id       VARCHAR2(50);
    l_offset          NUMBER;
    l_page_size       NUMBER;
    l_offset_idx      NUMBER;
    l_page_size_idx   NUMBER;
    l_blob_row_count  NUMBER;
    l_length          NUMBER;
    l_continue_loop   BOOLEAN;
    t_parameters      dwh_extract_parameter_tab;
    e_file_name       EXCEPTION;
    e_file_type       EXCEPTION;
    e_rest_failed     EXCEPTION; 
BEGIN

    g_proc := 'run_extract';

    g_step := 'Fetch settings';
    BEGIN
        SELECT objectstorageurl, credentialsname 
        INTO l_obj_storage_url, l_credentials 
        FROM con_etlsettings;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            p_error_message := 'ERP Settings not found';
        WHEN OTHERS THEN
            p_error_message := 'Error fetching erp settings: ' || SQLERRM;
    END;

    g_step := 'Get extract settings';
    BEGIN
        SELECT bi_report, oci_bucket_path, file_name, file_type, compress_flag
        INTO l_bi_report, l_obj_path, l_file_name, l_file_type, l_compress_flag
        FROM dwh_extract_setup
        WHERE id = p_extract_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            p_error_message := 'Extract settings not found';
        WHEN OTHERS THEN
            p_error_message := 'Error fetching extract settings: ' || SQLERRM;
    END;

    g_step := 'Validate file name and type';

    if l_file_name is null then 
        raise e_file_name;
    end if;

    if l_file_type is null or l_file_type not in ('CSV', 'XML') then
        raise e_file_type;
    end if;
    l_file_ext := '.'||l_file_type;

    t_parameters := p_parameters;
    
    g_step := 'Get offset and page_size';
    for i in 1..t_parameters.count loop
        if upper(t_parameters(i).name) = 'P_OFFSET' then
            l_offset_idx := i;
            l_offset := t_parameters(i).value;
        elsif upper(t_parameters(i).name) = 'P_PAGE_SIZE' then
            l_page_size_idx := i;
            l_page_size := t_parameters(i).value;
        end if;
    end loop;
   
    --create temp blob where to collect data from extract
    DBMS_LOB.CREATETEMPORARY(l_temp_blob, TRUE);

    l_continue_loop := true;

    g_step := 'Before run BI Report loop';
    --loop runs once if offeset or page size is not defined
    --loop stops when page is empty
    WHILE l_continue_loop LOOP
        BEGIN
            IF l_offset is null or l_page_size is null then --offset or page size parameter is not defined so run_report only once
                l_continue_loop := false;
            ELSE
                debug(p_msg_id,'l_offset:'|| l_offset || ' l_page_size:'||l_page_size);
            END IF;
            
            g_step := 'Run BI Report loop (offset:'||l_offset||')';
            DBO_ERP_UTILS_PKG.run_report_v2(
        	            p_path  => l_bi_report
        			   ,p_reportdata => l_clob_in
                       ,p_parameters => t_parameters            
        			   ,p_storage_type => l_file_type
        			   ,p_store_in_table => 'N'
        			   ,x_blob => l_blob
        			   ,x_xml => l_xml
        			   ,x_clob => l_clob
                       ,x_result => l_result
        			   ,x_message => p_error_message);   

        EXCEPTION
            WHEN OTHERS THEN
                l_continue_loop := false;
                p_error_message := 'Error running extract: ' || SQLERRM;
        END;

        IF l_result = 'Success' THEN

            IF l_offset is not null and l_page_size is not null then --only for reports which have pagging enabled
                g_step := 'Count rows in BI response';
                select count(1)
                into l_blob_row_count 
                from table(APEX_DATA_PARSER.PARSE(p_content => l_blob, p_file_type => APEX_DATA_PARSER.c_file_type_csv, p_detect_data_types=>'N', p_max_rows=>10));--counting limited to 10 rows for performnce

                if l_blob_row_count > 1 then

                    debug(p_msg_id,'BI report returned:'|| l_blob_row_count ||' rows. Continue with next page.');

                    if l_offset is not null and l_offset > 0 then
                        g_step := 'Remove header row';
                        remove_header_row(p_msg_id, p_blob => l_blob, p_length => l_length);
                    end if;

                    DBMS_LOB.APPEND(l_temp_blob, l_blob);
                 
                    --calculate new offset
                    l_offset := l_offset + l_page_size;
                    t_parameters(l_offset_idx).value := l_offset;
                
                else
                    debug(p_msg_id,'BI report returned:'|| l_blob_row_count ||'. Report end reached, stop fetching next page.');

                    --consider returned lob as empty, exit loop
                    l_continue_loop := false;
                end if;
            else
                --case when offset and page_size is not defined. exit loop after first cycle
                DBMS_LOB.APPEND(l_temp_blob, l_blob);
            end if;

        ELSE --error in run report, exit loop
            l_continue_loop := false;
        END IF;

    END LOOP;

    l_blob := l_temp_blob;
    DBMS_LOB.FREETEMPORARY(l_temp_blob);

    IF l_result = 'Success' THEN

        if nvl(l_compress_flag,'N') = 'Y' then
            g_step := 'Zip file';
            apex_zip.add_file (
                p_zipped_blob => l_zip_file,
                p_file_name   => l_file_name||l_file_ext,
                p_content     => l_blob);
            
            apex_zip.finish(p_zipped_blob => l_zip_file );

            l_file_ext := '.zip';
        end if;
        
        g_step := 'Upload file to bucket';
        DBMS_CLOUD.PUT_OBJECT(
                credential_name => l_credentials,
                object_uri => l_obj_storage_url||l_obj_path||'/'||l_file_name||l_file_ext,
                contents => l_zip_file); 
           
    ELSE
        RAISE e_rest_failed; 
    END IF;

    p_result := l_result;

EXCEPTION
    WHEN e_file_name THEN
        p_result := 'Error';
        p_error_message := 'File name not supported: '||l_file_name;
        log_error(p_msg_id, p_error_message);
        dbo_msg_pkg.update_outbound_status(p_msg_id,'ERROR');
    WHEN e_file_type THEN
        p_result := 'Error';
        p_error_message := 'File type not supported: '||l_file_type;
        log_error(p_msg_id, p_error_message);
        dbo_msg_pkg.update_outbound_status(p_msg_id,'ERROR');
    WHEN e_rest_failed THEN
        p_result := l_result;
        p_error_message := 'REST API call failed.'||p_error_message;
        log_error(p_msg_id, p_error_message);
    WHEN OTHERS THEN
        p_result := 'Error';
        p_error_message := SQLERRM || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
        log_error(p_msg_id, p_error_message);
        dbo_msg_pkg.update_outbound_status(p_msg_id,'ERROR');
END run_extract;


-- function to calculate extract parameter value
FUNCTION calculate_parameter_value(
    p_extract_name    in dwh_extract_setup.name%type,
    p_parameter_name  in varchar2,
    p_calc_attr1      in varchar2 DEFAULT NULL, 
    p_calc_attr2      in varchar2 DEFAULT NULL,
    p_calc_attr3      in varchar2 DEFAULT NULL
) return varchar2 is

l_parameter_value varchar2(100);

BEGIN

    CASE 
    WHEN p_extract_name = 'DWH_GL_JOURNAL_ENTRIES' and upper(p_parameter_name) = 'P_POSTED_DATE' THEN
        --parameter p_posted_date must be fixed count of days defined in parameter p_calc_attr1
        l_parameter_value := TO_CHAR(sysdate - to_number(p_calc_attr1),  'YYYY-MM-DD') || 'T00:00:00';

    WHEN p_extract_name = 'DWH_AP_SUBLEDGER' and upper(p_parameter_name) = 'P_POSTED_DATE' THEN
        --parameter p_posted_date must be fixed count of days defined in parameter p_calc_attr1
        l_parameter_value := TO_CHAR(sysdate - to_number(p_calc_attr1),  'YYYY-MM-DD') || 'T00:00:00';
   
    WHEN p_extract_name = 'DWH_AR_SUBLEDGER' and upper(p_parameter_name) = 'P_POSTED_DATE' THEN
        --parameter p_posted_date must be fixed count of days defined in parameter p_calc_attr1
        l_parameter_value := TO_CHAR(sysdate - to_number(p_calc_attr1),  'YYYY-MM-DD') || 'T00:00:00';
   
    WHEN p_extract_name = 'DWH_FA_SUBLEDGER' and upper(p_parameter_name) = 'P_POSTED_DATE' THEN
        --parameter p_posted_date must be fixed count of days defined in parameter p_calc_attr1
        l_parameter_value := TO_CHAR(sysdate - to_number(p_calc_attr1),  'YYYY-MM-DD') || 'T00:00:00';
   
    ELSE
        l_parameter_value := null;
    END CASE;

    RETURN l_parameter_value;

END calculate_parameter_value;



--This procedure is called from Apex Automation
PROCEDURE execute_extract_process(
    p_extract_name IN VARCHAR2
) IS
    
    l_extract_id number;
    l_msg_id     raw(16);
    l_running_msg_id     raw(16);
    l_param_value varchar2(150);
    l_bi_params  clob;
    l_payload    clob;
    l_result     varchar2(30);
    l_err_msg    varchar(32767);
    l_count      number;
    t_parameters dwh_extract_parameter_tab := dwh_extract_parameter_tab();
    l_step       varchar2(100);

    e_is_running exception;
    e_extract_not_found exception;
BEGIN

    apex_automation.log_info( p_message => 'Extract Name: '||p_extract_name);
    
    l_step := 'Query extract details';
    begin
        select id, bi_parameters
        into l_extract_id, l_bi_params
        from dwh_extract_setup
        where name = p_extract_name
        and enabled = 'Y';
    exception 
        when no_data_found then
            raise e_extract_not_found;
    end;

    apex_automation.log_info( p_message => 'Extract ID:'|| l_extract_id );

    if l_bi_params is not null then
        
        apex_automation.log_info( p_message => 'Default Parameters: ' ||l_bi_params);
        l_count :=0;
        
        l_step := 'Parse parameters json';
        FOR rec IN (SELECT jt.position, jt.name, jt.value, jt.calculate_flag, jt.calc_attr1, jt.calc_attr2, jt.calc_attr3
                    FROM dual,
                     JSON_TABLE(
                         l_bi_params,
                         '$.parameters[*]'
                             COLUMNS (position NUMBER PATH '$.position',
                                      name VARCHAR2(50) PATH '$.name',
                                      value VARCHAR2(50) PATH '$.value',
                                      calculate_flag VARCHAR2(1) PATH '$.calculate_flag',
                                      calc_attr1    VARCHAR2(1) PATH '$.calc_attr1',
                                      calc_attr2    VARCHAR2(1) PATH '$.calc_attr2',
                                      calc_attr3    VARCHAR2(1) PATH '$.calc_attr3')
                         ) jt
                    order by jt.position)
        LOOP
            l_count := l_count +1;
            l_param_value := null;
            if rec.calculate_flag = 'Y' then
                l_param_value := dwh_extract_pkg.calculate_parameter_value(
                                        p_extract_name => p_extract_name, 
                                        p_parameter_name => rec.name,
                                        p_calc_attr1 => rec.calc_attr1,
                                        p_calc_attr2 => rec.calc_attr2,
                                        p_calc_attr3 => rec.calc_attr3);
            else 
                l_param_value := rec.value;
            end if;
            l_payload := l_payload ||rec.position||'. '||rec.name||'='||l_param_value||chr(10);
            t_parameters.EXTEND;
            t_parameters(l_count) := dwh_extract_parameter_typ(rec.position, rec.name, l_param_value);
        END LOOP;
        
        apex_automation.log_info( p_message => 'Parameters: ' ||l_payload);
    else
        l_payload := 'Extract '|| p_extract_name || ' has no parameters.';
    end if;

     begin 
        select msg_id
        into l_running_msg_id
        from dbo_msg_outbound
        where msg_type = 'DWH'
        and msg_source_ref = p_extract_name
        and msg_status = 'PROCESSING'
        order by creation_date desc
        fetch first 1 row only;
    exception
        when no_data_found then
            null;
    end;

    l_step := 'Create outbound message';
    l_msg_id := sys_guid();
    dbo_msg_pkg.store_outbound_msg(
        p_msg_id   => l_msg_id,
        p_payload   => l_payload,
        p_msg_type  => 'DWH',
        p_source_ref => p_extract_name
    );

    apex_automation.log_info( p_message => 'Outbound MSG_ID: ' ||l_msg_id);

    if l_running_msg_id is not null then
        raise e_is_running;
    end if; 

    l_step := 'Update status to processing';
    dbo_msg_pkg.update_outbound_status(
        p_msg_id => l_msg_id,
        p_msg_status => 'PROCESSING');

    apex_automation.log_info( p_message => 'Processing ... ');

    l_step := 'Run extract';
    dwh_extract_pkg.run_extract(
        p_msg_id        => l_msg_id,
        p_extract_id    => l_extract_id,
        p_parameters    => t_parameters,
        p_result        => l_result, 
        p_error_message => l_err_msg);

    apex_automation.log_info( p_message => 'Status: ' ||l_result || '; Message: '||l_err_msg);

    if l_result = 'Success' then 

        dbo_msg_pkg.update_outbound_status(
            p_msg_id => l_msg_id,
            p_msg_status => 'PROCESSED');
    else
        dbo_msg_pkg.update_outbound_status(
            p_msg_id => l_msg_id,
            p_msg_status => 'ERROR');

        APEX_AUTOMATION.LOG_ERROR (p_message =>  l_err_msg);
        
    end if;

exception
    when e_extract_not_found then
        l_msg_id := sys_guid();
        dbo_msg_pkg.store_outbound_msg(
            p_msg_id   => l_msg_id,
            p_payload   => 'ERROR: extract '||p_extract_name||' not found.',
            p_msg_type  => 'DWH',
            p_source_ref => p_extract_name
        );

        dbo_msg_pkg.update_outbound_status(
                p_msg_id => l_msg_id,
                p_msg_status => 'ERROR');

        dbo_msg_pkg.log_error(l_msg_id, 'Extract '||p_extract_name||' is not found. Check if its enabled.', 'ERROR');
        APEX_AUTOMATION.LOG_ERROR (p_message => 'Extract '||p_extract_name||' is not found. Check if its enabled.');
        
    when e_is_running then

        dbo_msg_pkg.update_outbound_status(
                p_msg_id => l_msg_id,
                p_msg_status => 'ERROR');

        dbo_msg_pkg.log_error(l_msg_id, 'Previous instance (MSG_ID '||l_running_msg_id||') is still running.', 'ERROR');
        APEX_AUTOMATION.LOG_ERROR (p_message => 'Previous instance (MSG_ID '||l_running_msg_id||') is still running.');

    when others then
        
        dbo_msg_pkg.update_outbound_status(
                p_msg_id => l_msg_id,
                p_msg_status => 'ERROR');

        dbo_msg_pkg.log_error(l_msg_id, substr(l_step||'>> Unexpected Error: '|| SQLERRM,1,4000), 'ERROR');
        APEX_AUTOMATION.LOG_ERROR (p_message => l_step||'>> Unexpected Error: '|| SQLERRM);

        
END execute_extract_process;

function is_running (
    p_application_id IN number, 
    p_static_id IN varchar2
) return varchar2 is
begin
    if apex_automation.is_running(p_application_id => p_application_id, p_static_id => p_static_id) then
        return 'Y';
    else 
        return 'N';
    end if;
end;

end "DWH_EXTRACT_PKG";
/