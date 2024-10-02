create or replace PACKAGE BODY                      "ENS_HIERARCHIES_PKG" as
--*****************************************************************************
--Module      : ENS - Enterprise Structures
--Type        : PL/SQL - Package
--Author      : Aldis Lagzdins
--Version     : 1.0
--
--
-- Description: Manage Segment Hierachies - validation, upload to OracleERP, 
-- prepare extracts, procedure executed from workflow activities
--
-- *****************************************************************************

-- -------------------------------------------------
-- Change log
-- Date        Author          Version     Comment
----------------------------------------------------
-- 28.07.2024  Aldis Lagzdins  1.0         Created
-- -------------------------------------------------

/*===========================================
============= Global Variables ==============
===========================================*/

g_pkg           constant varchar2(20)  := 'ENS_HIERARCHIES_PKG';
g_proc                   varchar2(50)  := '';
g_step                   varchar2(100) := '';

/*===========================================
==== PRIVATE Procedures and Functions =======
===========================================*/

--Log and debug
PROCEDURE log(
    p_msg_id  in dbo_msg_inbound.msg_id%type,
    p_message in varchar2,
    p_status  in varchar2 default 'ERROR'
) IS 
    l_msg_pref varchar2(200);
BEGIN
    l_msg_pref := g_pkg||'.'||g_proc||'>>'||g_step||':';
    if p_msg_id is not null then --does not support error loging without MSG_ID
        dbo_msg_pkg.log_error(p_msg_id, substr(l_msg_pref||p_message,1,2000), p_status);
        if p_message is not null then
            dbo_msg_pkg.debug_msg(p_msg_id,'ENS',substr(l_msg_pref||p_message,1,2000));
        end if;
    end if;
END log;

--Update status in all ENS hierachies tables
PROCEDURE update_status(
    p_id         in ens_segment_hierarchy_uploads.id%type,
    p_status     in ens_segment_hierarchy_uploads.status%type,
    p_status_msg in ens_segment_hierarchy_uploads.status_msg%type
) IS
BEGIN

    if p_id is not null then

        update ens_segment_hierarchy_uploads
        set status = p_status, status_msg = p_status_msg, last_update_date = sysdate
        where id = p_id;
        commit;
    end if;
END;

--Create CSV based on interface table name
PROCEDURE generate_dynamic_csv (
    p_tree_code    in  ens_gl_segment_hier_int.tree_code%type,
    p_line_ending  in  varchar2 default NULL,
    p_csv_output   out clob
) IS
    l_sql                varchar2(32767);
    l_col_list           varchar2(32767);
    l_col_header         varchar2(32767);
    l_csv_output         clob;
    l_first_col          boolean         := TRUE;
    l_add_header         boolean         := FALSE;
    l_tree_vesrion_base  varchar2(50);

    l_col_sep      CONSTANT VARCHAR2(1)  := ',';
    l_col_concat   CONSTANT VARCHAR2(10) := q'[||','||]';

    c_csv SYS_REFCURSOR;
    
    l_data_row VARCHAR2(32767);
    l_col_count INTEGER := 0;

BEGIN
    g_proc := 'generate_dynamic_csv';

     --Initialize CLOB to collect CSV rows
    DBMS_LOB.CREATETEMPORARY(l_csv_output, TRUE);

    g_step := 'construct sql';
    l_col_list := q'[a.value_set_code||','||a.tree_code||','||b.tree_version_name||','||
                     to_char(b.effective_start_date,'YYYY/MM/DD')||','||
                     to_char(b.effective_end_date,'YYYY/MM/DD')||','||
                     COALESCE(value, parent1, parent2, parent3, parent4, parent5, parent6, parent7, parent8, parent9)||','||
                     NVL((select COALESCE(p.parent1, p.parent2, p.parent3, p.parent4, p.parent5, p.parent6, p.parent7, p.parent8, p.parent9) from ENS_GL_SEGMENT_HIER_INT p where p.x_id = a.x_parent_id),'None')||','||
                     case when value is not null then 31
                          when PARENT1 is not null then 30
                          when PARENT2 is not null then 29
                          when PARENT3 is not null then 28
                          when PARENT4 is not null then 27
                          when PARENT5 is not null then 26
                          when PARENT6 is not null then 25
                          when PARENT7 is not null then 24
                          when PARENT8 is not null then 23
                          when PARENT9 is not null then 22
                          when PARENT10 is not null then 21 END ||','||
                      label_short_name]';
    
    -- Construct the SQL query to fetch data
    l_sql := 'SELECT '|| l_col_list ||' as data '||
             ' FROM ens_gl_segment_hier_int a, ens_tree_versions b'||
             ' WHERE a.tree_code = '''||p_tree_code||''''||
             ' AND a.tree_code = b.tree_code'||
             ' AND b.upload_flag = ''Y'''||
             ' AND a.x_data_source = ''ORAFIN'''||
             ' ORDER BY a.tree_version_name, x_order ASC'
             ;
    
    --add heaader
    IF l_add_header THEN
        DBMS_LOB.WRITEAPPEND(l_csv_output, LENGTH(l_col_header), l_col_header || CHR(10));
    END IF;

    g_step := 'execute sql and write csv';
    -- Open the cursor and fetch data
    OPEN c_csv FOR l_sql;
    LOOP
        FETCH c_csv INTO l_data_row;
        EXIT WHEN c_csv%NOTFOUND;

        DBMS_LOB.WRITEAPPEND(l_csv_output, LENGTH(l_data_row||p_line_ending|| CHR(10) ), l_data_row||p_line_ending|| CHR(10) );
    END LOOP;
    CLOSE c_csv;

    -- Free temporary CLOB
    IF DBMS_LOB.ISTEMPORARY(p_csv_output) = 1 THEN
        DBMS_LOB.FREETEMPORARY(p_csv_output);
    END IF;

    p_csv_output := l_csv_output;

EXCEPTION
    WHEN OTHERS THEN
        IF c_csv%ISOPEN THEN
            CLOSE c_csv;
        END IF;

        -- Free temporary CLOB
        IF DBMS_LOB.ISTEMPORARY(p_csv_output) = 1 THEN
            DBMS_LOB.FREETEMPORARY(p_csv_output);
        END IF;

        RAISE;
END generate_dynamic_csv;


--Create new upload to ERP. Invoked from WF.
PROCEDURE create_erp_upload (
    p_msg_id in dbo_msg_inbound.msg_id%type,
    p_tree_version in varchar2,
    p_wf_instance_id in varchar2,
    p_user in varchar2,
    p_upload_id out number,
    p_status out varchar2,
    p_status_msg out varchar2
) IS
BEGIN
    g_proc := 'create_erp_upload';
    g_step := 'Insert new record';

    p_status := 'PROCESSING';

    INSERT INTO ens_segment_hierarchy_uploads (msg_id, tree_version_name, status, upload_start, creation_date, created_by, last_update_date, last_updated_by, wf_instance_id) 
    VALUES (p_msg_id, p_tree_version, p_status, sysdate, sysdate, p_user, sysdate, p_user, p_wf_instance_id)
    RETURNING id INTO p_upload_id;

EXCEPTION
    WHEN OTHERS THEN
        p_status_msg:=SQLERRM;
        p_status:='ERROR';
        if p_msg_id is not null then
          log(p_msg_id, p_status_msg, p_status);
        end if;
        
        update_status(p_upload_id,p_status,p_status_msg);
        RAISE;
END create_erp_upload;

--Store zip file blob and base64 ready for sending to ERP
PROCEDURE save_out_file(
    p_msg_id         IN dbo_msg_inbound.msg_id%type,
    p_upload_id      in ens_segment_hierarchy_uploads.id%type,
    p_zip_file       in blob,
    p_wf_instance_id in ens_segment_hierarchy_uploads.wf_instance_id%type)
IS
    l_file_as_base64 clob;

BEGIN
    g_proc := 'save_out_file';

    -- Initialize CLOB to store base64 of the zip file
    DBMS_LOB.CREATETEMPORARY(l_file_as_base64, TRUE);

    g_step := 'convert to base64';
    l_file_as_base64 := apex_web_service.blob2clobbase64(p_zip_file);

    --  Remove New Line and Carriage Return in base64
    l_file_as_base64 := REPLACE(l_file_as_base64, CHR(10), '');
    l_file_as_base64 := REPLACE(l_file_as_base64, CHR(13), '');
    

    g_step := 'save file to dbo_msg_inbound table';
    UPDATE dbo_msg_inbound
    SET msg_payload = l_file_as_base64
      , last_update_date = sysdate
    where msg_id = p_msg_id;

    -- Free temporary CLOB
    IF DBMS_LOB.ISTEMPORARY(l_file_as_base64) = 1 THEN
        DBMS_LOB.FREETEMPORARY(l_file_as_base64);
    END IF;

    EXCEPTION
        WHEN others THEN
        -- Free temporary CLOB
        IF DBMS_LOB.ISTEMPORARY(l_file_as_base64) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_file_as_base64);
        END IF;
        RAISE;
END save_out_file;


-- Prepare CSV and Zip it before sending to ERP. Invoked form WF.
PROCEDURE zip_csv_file (
    p_msg_id         in dbo_msg_inbound.msg_id%type,
    p_upload_id      in ens_segment_hierarchy_uploads.id%type,
    p_wf_instance_id in ens_segment_hierarchy_uploads.wf_instance_id%type,
    p_status         out varchar2,
    p_status_msg     out varchar2
) IS
    l_tree_version    ens_segment_hierarchy_uploads.tree_version_name%type;
    l_tree_code       ens_gl_segment_hier_int.tree_code%type;
    l_hier_csv_output clob;
    l_csv_raw         raw(32000);
    l_hier_csv_file   blob;
    l_csv_file        blob;
    l_zip_file        blob;

    l_gl_seg_hier_filename constant varchar2(30) := 'GlSegmentHierInterface.csv';

    e_empty_csv EXCEPTION;

begin
    g_proc := 'zip_csv_file';

    g_step := 'set wf id';
    update ens_segment_hierarchy_uploads
    set wf_instance_id = p_wf_instance_id, last_update_date = sysdate
    where id = p_upload_id;

    g_step := 'get tree code and version name';

    select a.tree_version_name, b.tree_code
    into l_tree_version, l_tree_code
    from ens_segment_hierarchy_uploads a, ens_tree_versions b
    where a.tree_version_name = b.tree_version_name 
    and b.current_flag = 'Y'
    and a.id = p_upload_id;

    --Prepare hierachy file
    generate_dynamic_csv(
        p_tree_code => l_tree_code,
        p_line_ending  => ',',
        p_csv_output   => l_hier_csv_output);

    IF length(l_hier_csv_output) = 0 THEN
        RAISE e_empty_csv;
    ELSE

        g_step := 'add csv to zip file';

        --Initialize BLOB that will be zipped later
        DBMS_LOB.CREATETEMPORARY(l_hier_csv_file, TRUE);

        l_hier_csv_file := APEX_UTIL.CLOB_TO_BLOB (p_clob => l_hier_csv_output);
        
        --add RA Lines csv file to zip
        apex_zip.add_file (
            p_zipped_blob => l_zip_file,
            p_file_name   => l_gl_seg_hier_filename,
            p_content     => l_hier_csv_file);

        --Close zip 
        apex_zip.finish(p_zipped_blob => l_zip_file );

        g_step := 'store out file';
        --Store out file ready for sending to ERP
        save_out_file(p_msg_id, p_upload_id, l_zip_file, p_wf_instance_id);

    END IF;

    -- Free temporary BLOB
    IF DBMS_LOB.ISTEMPORARY(l_hier_csv_file) = 1 THEN
        DBMS_LOB.FREETEMPORARY(l_hier_csv_file);
    END IF;

EXCEPTION
    WHEN e_empty_csv THEN
        -- Free temporary BLOB
        IF DBMS_LOB.ISTEMPORARY(l_hier_csv_file) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_hier_csv_file);
        END IF;

        p_status_msg:='zip_csv_files: Empty CSV';
        p_status:='ERROR';
        if p_msg_id is not null then
          log(p_msg_id, p_status_msg, p_status);
        end if;
        
        update_status(p_upload_id,p_status,p_status_msg);

        RAISE_APPLICATION_ERROR(-20001, p_status_msg);
        
    WHEN OTHERS THEN
        -- Free temporary BLOB
        IF DBMS_LOB.ISTEMPORARY(l_hier_csv_file) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_hier_csv_file);
        END IF;

        p_status_msg:=SQLERRM;
        p_status:='ERROR';
        if p_msg_id is not null then
          log(p_msg_id, p_status_msg, p_status);
        end if;

        update_status(p_upload_id,p_status,p_status_msg);
        RAISE;
END zip_csv_file;


--Takes hierarchies payload (CSV) from dbo_msg_inbound, converts to base64 and uploads to UCM. Invoked from WF. 
PROCEDURE upload_file_to_ucm (
    p_msg_id     in dbo_msg_inbound.msg_id%type,
    p_upload_id  in ens_segment_hierarchy_uploads.id%type
) IS
    l_status_msg    varchar2(4000);
    l_status        varchar2(30);
    l_status_code   number;
    l_file_contents clob;
    l_document_id   number;
    l_file_name     varchar2(100);
    l_file_ext      varchar2(3) := 'zip';

    e_upload_error  exception;

BEGIN
    
    g_proc := 'upload_file_to_ucm';

    g_step := 'get file content';

    begin 
        select dmi.msg_payload, 'Tree_Version_'||replace(hie.tree_version_name,' ','_') ||'_'|| to_char(hie.upload_start,'DD-MM-YYYY_HH24:MI:SS') || '.' || l_file_ext
        into l_file_contents, l_file_name
        from dbo_msg_inbound dmi, ens_segment_hierarchy_uploads hie
        where dmi.msg_id = p_msg_id
        and hie.msg_id = dmi.msg_id
        and hie.id = p_upload_id;
    exception
        when no_data_found then
            raise_application_error(-20010, 'Message not found for MSG_ID: '|| p_msg_id);
        when too_many_rows then
            raise_application_error(-20010, 'Too many records found for MSG_ID: '|| p_msg_id);
    end;
    
    g_step := 'invoke upload to ucm';

    dbo_erp_utils_pkg.rest_upload_file_to_ucm(
        p_msg_id            => p_msg_id,
        p_file_contents     => l_file_contents,
        p_document_account  => 'fin/generalLedger/import',
	    p_module			=> 'GL',
	    p_rest_endpoint	    => '/fscmRestApi/resources/11.13.18.05/erpintegrations',
        p_file_name         => l_file_name,
        p_content_type      => l_file_ext,
        p_encode_to_base64  => FALSE,
        p_status_code       => l_status_code,
        p_document_id       => l_document_id,
        p_error_message     => l_status_msg);

    g_step := 'update upload record';

    if l_status_code = 201 then 
        update ens_segment_hierarchy_uploads
        set ucm_document_id = l_document_id, status_msg = 'Uploaded to UCM. Document ID='|| l_document_id, last_update_date = sysdate
        where id = p_upload_id;
    else 
        raise e_upload_error;
    end if; 

EXCEPTION
    WHEN e_upload_error THEN
        update_status(p_upload_id,'ERROR', 'Status code: '||l_status_code||l_status_msg);
        log(p_msg_id, 'Status code: '||l_status_code||l_status_msg, 'ERROR');
        RAISE;
    WHEN others THEN
        l_status_msg := SQLERRM;
        update_status(p_upload_id,'ERROR', l_status_msg);
        log(p_msg_id, l_status_msg, 'ERROR');
        RAISE;
END upload_file_to_ucm;

--Start ERP load process to import data from the uploaded zip file. Invoked from WF.
PROCEDURE submit_erp_interface_loader(
    p_msg_id      in  dbo_msg_inbound.msg_id%type,
    p_upload_id   in  ens_segment_hierarchy_uploads.id%type
) IS
  l_error_message VARCHAR2(255);
  l_status_code NUMBER(10);
  l_document_id ens_segment_hierarchy_uploads.ucm_document_id%type;
  l_request_id ens_segment_hierarchy_uploads.erp_request_id%type;

  e_doc_id_missing exception;
  e_loader_ess     exception;
  e_erp_offline    exception;
BEGIN
    g_proc := 'submit_erp_interface_loader';
  
    IF dbo_erp_utils_pkg.erp_is_online THEN

        g_step := 'get document id';

        select ucm_document_id
        into l_document_id
        from ens_segment_hierarchy_uploads
        where id = p_upload_id;
    
        IF l_document_id IS NULL THEN
            raise e_doc_id_missing;
        ELSE

            g_step := 'submit erp loader process';

            --start ERP interfaceLoader ESS job 
            dbo_erp_utils_pkg.rest_submit_ess_job_request_callback_ct(
                p_msg_id           => p_msg_id,
                p_module           => 'ENS',
                p_rest_endpoint    => '/fscmRestApi/resources/11.13.18.05/erpintegrations',
                p_job_package_name => 'oracle/apps/ess/financials/commonModules/shared/common/interfaceLoader/',
                p_job_def_name     => 'InterfaceLoaderController',
                p_parameter_str    => '16'||','||l_document_id||',N,N',
                p_status_code      => l_status_code,
                p_requestid        => l_request_id,
                p_error_message    => l_error_message,
                p_callback         => '/erpCallBack/callBack',
                p_ct               => 'HIE');

            IF l_status_code=201 and l_request_id IS NOT NULL THEN
                
                update ens_segment_hierarchy_uploads
                set erp_request_id = l_request_id,
                    last_update_date = sysdate
                where id = p_upload_id
                and ucm_document_id = l_document_id;

                update_status(p_upload_id,'WAITING', 'Waiitng ERP to complete import. Process Id='||l_request_id);
            ELSE
                raise e_loader_ess;
            END IF;
        END IF;
    ELSE
        raise e_erp_offline;
    END IF;
EXCEPTION
    WHEN e_doc_id_missing THEN
        update_status(p_upload_id,'ERROR', 'Document ID is missing.');
        log(p_msg_id,'Document ID is missing.','ERROR');
        RAISE_APPLICATION_ERROR(-20001, 'Document ID is missing.');
    WHEN e_loader_ess THEN           
        update_status(p_upload_id,'ERROR', l_status_code||':'||l_error_message);
        log(p_msg_id,l_status_code||':'||l_error_message,'ERROR');
        RAISE_APPLICATION_ERROR(-20001, 'Error running ERP Loader ESS (request_id='||l_request_id||'). Error: '||l_status_code||':'||l_error_message);
    WHEN e_erp_offline THEN
        update_status(p_upload_id,'ERROR', 'ERP is in Offline state. When Online resume workflow from AR Application.');
        log(p_msg_id, 'ERP is in Offline state. When Online resume workflow from AR Application.');
        RAISE_APPLICATION_ERROR(-20001, 'ERP is in Offline state. When Online resume workflow from AR Application.');
    WHEN others THEN
        update_status(p_upload_id,'ERROR', SQLERRM);
        log(p_msg_id, SQLERRM);
        RAISE;
END submit_erp_interface_loader;


--Process ERP ESS Job CallBack message. Invoked from dbo_erp_utils_pkg.store_erp_callback
PROCEDURE process_erp_callback(
    p_msg_id      in dbo_callback_msg.msg_id%type,
    p_callback_id in dbo_callback_msg.callback_id%type
) IS 
    l_json                  clob;
    l_request_id            number;
    l_status                varchar2(10);
    l_error                 varchar2(30) := 'ERROR';
    l_activity_params       wwv_flow_global.vc_map;
    l_wf_instance_id        ens_segment_hierarchy_uploads.wf_instance_id%type;
    l_security_group_id     number;
    l_callback_id           number;
    l_wf_activity_static_id VARCHAR2(50) := 'ENS_WAIT_ERP_HIERARCHIES_IMPORT';
    l_upload_id             ens_segment_hierarchy_uploads.id%type;
    l_status_msg            ens_segment_hierarchy_uploads.status_msg%type;
    l_created_by            ens_segment_hierarchy_uploads.created_by%type;
    l_app_id                apex_workflows.application_id%type;
    l_cnt                   number := 0;

    e_wf_not_found          exception;
    e_upload_not_found      exception;

BEGIN
    g_proc := 'process_erp_callback';

    g_step := 'query upload record';

    begin
        select u.id, u.wf_instance_id, u.created_by
        into l_upload_id, l_wf_instance_id, l_created_by
        from ens_segment_hierarchy_uploads u
        where u.msg_id = p_msg_id;

    exception
        when no_data_found then
            raise e_upload_not_found;
    end;

    g_step := 'get workflow app id';

    l_app_id := dbo_msg_pkg.get_workflow_app_id(l_wf_instance_id);

    if l_app_id is null then
        raise e_wf_not_found;
    end if;

    g_step := 'query callback message';

    select json_content
    into l_json 
    from dbo_callback_msg
    where callback_id = p_callback_id;


    IF l_json is not null THEN
        BEGIN
            g_step := 'Parse callback json inside xml';

            FOR rec IN (select jt.RequestId, jt.Status, jt.JobName, jt.ChildRequestId, jt.ChildStatus, jt.ChildJobName
                        from dual, 
                         JSON_TABLE(
                           l_json,
                           '$'
                           COLUMNS (
                             NESTED PATH '$.JOBS[*]'
                               COLUMNS (
                                   DocumentName VARCHAR2(240) PATH '$.DOCUMENTNAME',
                                   JobName VARCHAR2(240) PATH '$.JOBNAME',
                                   RequestId NUMBER PATH '$.REQUESTID',
                                   Status VARCHAR2(10) PATH '$.STATUS',
                                   NESTED PATH '$.CHILD[*]'
                                   COLUMNS (
                                        ChildJobName VARCHAR2(240) PATH '$.JOBNAME',
                                        ChildRequestId NUMBER PATH '$.REQUESTID',
                                        ChildStatus VARCHAR2(10) PATH '$.STATUS'
                                    )
                                )
                           )
                         ) jt)
            LOOP
                l_cnt := l_cnt + 1;

                if l_cnt = 1 then
                    l_status_msg:= rec.RequestId ||' '|| rec.Status ||' '|| rec.JobName|| CHR(10);
                end if;

                if rec.Status != 'SUCCEEDED' or rec.ChildStatus != 'SUCCEEDED' then
                    l_status:='ERROR';
                else 
                    l_status:='SUCCEEDED';
                end if;
                l_status_msg := l_status_msg||'    '||rec.ChildRequestId ||' '|| rec.ChildStatus ||' '|| rec.ChildJobName|| CHR(10);
            
            END LOOP;             

        EXCEPTION
            WHEN others THEN
                l_error := 'ERR_JSON';
                RAISE;
        END;

        update ens_segment_hierarchy_uploads
        set last_update_date=sysdate, erp_request_status=l_status, status_msg = l_status_msg
        where id = l_upload_id;

    END IF;

    g_step := 'Continue workflow wait activity';
    BEGIN

        apex_session.create_session (
            p_app_id   => l_app_id,
            p_page_id  => 1,
            p_username => l_created_by );

         l_security_group_id := apex_util.find_security_group_id (p_workspace => 'XX_INTEGRATION_DEV');
         apex_util.set_security_group_id (p_security_group_id => l_security_group_id);
         
         --l_activity_result('TASK_OUTCOME')   :=  'APPROVED';
         apex_workflow.continue_activity(
              p_instance_id          => l_wf_instance_id,
              p_static_id            => l_wf_activity_static_id,
              p_activity_params      => l_activity_params
              );

    EXCEPTION
        WHEN others THEN
           l_error := 'ERR_WAIT';
           RAISE;
    END;

EXCEPTION
    when e_upload_not_found then
        update_status(l_upload_id, 'ERROR', 'ENS Hierarchies upload record not found.');
        log(p_msg_id, 'ENS Hierarchies upload record not found.', 'ERROR');
        raise;
    when e_wf_not_found then
        update_status(l_upload_id, 'ERROR', 'ENS Hierarchies workflow not found.');
        log(p_msg_id, 'ENS Hierarchies workflow not found.', 'ERROR');
        raise;
    when others then
        update_status(l_upload_id, 'ERROR', SQLERRM);
        log(p_msg_id, SQLERRM, 'ERROR');
        raise;
END process_erp_callback;

--final validations and set completion status. Invoked from WF.
PROCEDURE set_final_status (
    p_msg_id      in  dbo_msg_inbound.msg_id%type,
    p_upload_id   in  ens_segment_hierarchy_uploads.id%type
) IS

    l_status                ens_segment_hierarchy_uploads.status%type;
    l_status_msg            ens_segment_hierarchy_uploads.status_msg%type;
    l_erp_request_status    ens_segment_hierarchy_uploads.erp_request_status%type;
    l_erp_request_id        ens_segment_hierarchy_uploads.erp_request_id%type;

BEGIN
    g_proc := 'set_final_status';

    g_step := 'query upload record';

    select status, erp_request_status, erp_request_id, status_msg
    into l_status, l_erp_request_status, l_erp_request_id, l_status_msg
    from ens_segment_hierarchy_uploads
    where id = p_upload_id;

    g_step := 'decide final status and message';
    if l_erp_request_status is null then
        l_status := 'ERROR';
        l_status_msg := 'Oracle ERP process "Load Interface File for Import" status is unknown. Please inspect Oracle ERP process (Process ID='||l_erp_request_id||') logs for more details.';
        log(p_msg_id, l_status_msg, l_status);

    elsif l_erp_request_status != 'SUCCEEDED' then
        l_status := 'ERROR';
        l_status_msg := 'Please inspect following Oracle ERP process logs.'||CHR(10)||l_status_msg;
        log(p_msg_id, l_status_msg, l_status);
    else
        l_status := 'PROCESSED';
        l_status_msg := 'Workflow completed successfully! In case of issues please inspect Oracle ERP process logs (Process ID='||l_erp_request_id||') for more details.';
    end if;
    
    update_status(p_upload_id, l_status, l_status_msg);

    UPDATE dbo_msg_inbound
    SET msg_status = l_status
      , last_update_date = sysdate
    where msg_id = p_msg_id;

EXCEPTION
    when others then
        update_status(p_upload_id, 'ERROR', SQLERRM);
        log(p_msg_id, SQLERRM, 'ERROR');
        raise;
END set_final_status;

/*Procedure to add new value in segment hierarchy. 
    IN: all inputs are mandatory. 
    OUT: p_result - S-success, E-error.
    OUT: p_result_msg - status details.
*/
PROCEDURE add_value_to_hierarchy(
    p_data_source    IN ens_gl_segment_hier_int.x_data_source%type,
    p_value_set_name IN ens_gl_coa_info.value_set_name%type,
    p_value          IN VARCHAR2,
    p_parent_value   IN VARCHAR2,
    p_result         OUT VARCHAR2,
    p_result_msg     OUT VARCHAR2
) IS

    l_value_set_name     ens_gl_coa_info.value_set_name%type;
    l_parent_id          ens_gl_segment_hier_int.x_parent_id%type;
    l_tree_version_name  ens_gl_segment_hier_int.tree_version_name%type;
    l_id                 ens_gl_segment_hier_int.x_id%type;
    l_value_parent_id    ens_gl_segment_hier_int.x_parent_id%type;
    l_x_order            ens_gl_segment_hier_int.x_order%type;
    l_parent1            ens_gl_segment_hier_int.parent1%type;

    e_raise_error exception;
BEGIN

    if p_data_source is null then
        p_result := 'E';
        p_result_msg := 'Data source (p_data_source) is mandatory.';
        raise e_raise_error;
    elsif p_value_set_name is null then
        p_result := 'E';
        p_result_msg := 'Value set name (p_value_set_name) is mandatory.';
        raise e_raise_error;
    elsif p_value is null then
        p_result := 'E';
        p_result_msg := 'Value (p_value) is mandatory.';
        raise e_raise_error;
    elsif p_parent_value is null then
        p_result := 'E';
        p_result_msg := 'Parent value (p_parent_value) is mandatory.';
        raise e_raise_error;
    end if;

    begin

        select value_set_name
        into l_value_set_name
        from ens_gl_coa_info
        where value_set_name = p_value_set_name;
    exception
        when no_data_found then
            p_result := 'E';
            p_result_msg := 'Value set not found for value set '|| p_value_set_name||'.';
            raise e_raise_error;
    end;

    l_parent_id := null;

    begin
        -- get value id and parent id if values already exists in hierarchy
        select x_id, x_parent_id
        into l_id, l_value_parent_id
        from ens_gl_segment_hier_int
        where value = p_value
        and x_data_source = upper(p_data_source)
        and value_set_code = l_value_set_name
        and sysdate between tree_version_start_date_active and tree_version_end_date_active;

    exception
        when too_many_rows then
            p_result := 'E';
            p_result_msg := 'Found more than one occasion of '||p_value||' in hierarchy for '|| p_value_set_name||'.';
            raise e_raise_error;
        when no_data_found then
            null;
    end;

    if l_value_parent_id is not null then
        --get parent value from hierarchy if input value already exists in hierarchy
        select parent1
        into l_parent1
        from  ens_gl_segment_hier_int
        where x_id = l_value_parent_id;

        --if input parent value equal to parent in hierarchy then nothing to do and can exit
        if p_parent_value = l_parent1 then 
        
            p_result := 'S';
            p_result_msg := 'Value '||p_value||' with parent '||p_parent_value||' already exists in hierarchy for '|| p_value_set_name||'.';
            raise e_raise_error;

        else
            p_result_msg := 'Value exists in hierarchy but with different parent ('||l_parent1||'). Removing value from hierarchy and adding new. ';
        end if;
    end if;

    begin
        select x_id, tree_version_name
        into l_parent_id, l_tree_version_name
        from ens_gl_segment_hier_int
        where parent1 = p_parent_value
        and x_data_source = upper(p_data_source)
        and value_set_code = l_value_set_name
        and sysdate between tree_version_start_date_active and tree_version_end_date_active;

    exception
        when no_data_found then
            p_result := 'E';
            p_result_msg := 'Parent value '||p_parent_value||' not found in hierarchy for '|| p_value_set_name||'.';
            raise e_raise_error;
    end;
    
    --if it came so far then input value exists in hierarchy but input parent is not the same as in hierarchy. 
    --Need to delete value before adding new value from inputs.
    if l_value_parent_id is not null and l_parent1 is not null then
        delete from ens_gl_segment_hier_int
        where x_id = l_id
        and x_parent_id = l_value_parent_id;
    end if;

    --get new value order number
    select max(x_order) + 1
    into l_x_order 
    from ens_gl_segment_hier_int
    where x_parent_id = l_parent_id;

    --update order to the rest of tree records
    update ens_gl_segment_hier_int
    set x_order = x_order + 1
    where x_order >= l_x_order
    and value_set_code = l_value_set_name
    and x_data_source = upper(p_data_source)
    and sysdate between tree_version_start_date_active and tree_version_end_date_active;

    --insert new value based on parent record details
    insert into ens_gl_segment_hier_int (value_set_code, tree_code, tree_version_name, tree_version_start_date_active, tree_version_end_date_active, x_data_source, x_parent_id, value, x_order)
    select value_set_code, tree_code, tree_version_name, tree_version_start_date_active, tree_version_end_date_active, x_data_source, l_parent_id, p_value, l_x_order
    from ens_gl_segment_hier_int
    where x_id = l_parent_id
    and value_set_code = l_value_set_name
    and x_data_source = upper(p_data_source)
    and sysdate between tree_version_start_date_active and tree_version_end_date_active;

    if SQL%ROWCOUNT = 0 then 
        p_result := 'E';
        p_result_msg := 'New value '||p_value|| ' was not added to hierarchy for segment '||p_value_set_name||'.';
        raise e_raise_error;
    else

        p_result := 'S';
        p_result_msg := p_result_msg||'Value '||p_value|| ' succesfully added to hierarchy for segment '||p_value_set_name||'.';

    end if;
    
EXCEPTION
    WHEN e_raise_error THEN
        RETURN;
    WHEN others THEN
        p_result := 'E';
        p_result_msg := 'Something went wrong while adding value to hierarchy. Error details: '||SQLERRM;
END add_value_to_hierarchy;


-- Function to check if a value is the segment hierarchy. Returns Y
-- if the value is placed in the hierarchy, N otherwise.
FUNCTION is_value_in_hierarchy(
    p_data_source    IN ens_gl_segment_hier_int.x_data_source%type,
    p_value_set_name IN ens_gl_coa_info.value_set_name%type,
    p_value          IN VARCHAR2
) RETURN VARCHAR2 
IS
    l_result             VARCHAR2(1);
BEGIN
    begin
        select 'Y'
        into l_result
        from ens_gl_segment_hier_int
        where value = p_value
        and x_data_source = upper(p_data_source)
        and value_set_code = p_value_set_name
        and sysdate between tree_version_start_date_active and tree_version_end_date_active;
    exception
        when too_many_rows then
          l_result := 'Y';
        when no_data_found then
          l_result := 'N';
    end;

    return l_result;
END is_value_in_hierarchy;

--Refresh tree version data from Orafin. If tree version exists then update only effective dates, 
--if version doesnt exist then inser new row by default setting current=N, upload=N 
procedure load_tree_versions(p_result OUT VARCHAR2, p_error_message OUT VARCHAR2) IS  
    l_clob           CLOB;
	l_clob_in        clob;
    l_blob           BLOB;
    l_xml            XMLTYPE;
    l_bi_catalog     VARCHAR2(255);
    l_object_storage VARCHAR2(255);  
    l_credentials    VARCHAR2(255);
    l_result         VARCHAR2(50);
    l_ledger_id      VARCHAR2(50);
    e_rest_failed    EXCEPTION; 
BEGIN

    -- Fetch settings
    BEGIN
        SELECT bicatalog, objectstorageurl, credentialsname INTO l_bi_catalog, l_object_storage, l_credentials 
        FROM con_etlsettings;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            p_error_message := 'Settings not found';
        WHEN OTHERS THEN
            p_error_message := 'Error fetching settings: ' || SQLERRM;
    END;

    -- Run the report
    BEGIN
      DBO_ERP_UTILS_PKG.run_report(
    	            p_path  => l_bi_catalog||'ENS/KDAAccountingTreeVersionsREP.xdo'
    			   ,p_reportdata => l_clob_in
    			   ,p_storage_type => 'XML'
    			   ,p_store_in_table => 'N'
    			   ,x_blob => l_blob
    			   ,x_xml => l_xml
    			   ,x_clob => l_clob
                   ,x_result => l_result
    			   ,x_message => p_error_message);   
		 
    EXCEPTION
        WHEN OTHERS THEN
            p_error_message := 'Error running report: ' || SQLERRM;
    END;

    IF l_result = 'Success' THEN
            
        MERGE INTO ens_tree_versions etv
        USING (
            WITH xml_data AS (
                SELECT l_xml AS xml_col
                FROM dual
            )
            SELECT 
                x.TREE_NAME,
                x.TREE_CODE,
                x.TREE_VERSION_NAME,
                x.EFFECTIVE_START_DATE,
                x.EFFECTIVE_END_DATE
            FROM xml_data,
            XMLTABLE('/DATA_DS/G_1'
                PASSING xml_data.xml_col
                COLUMNS 
                    TREE_NAME VARCHAR2(50) PATH 'TREE_NAME',
                    TREE_CODE VARCHAR2(50) PATH 'TREE_CODE',
                    TREE_VERSION_NAME VARCHAR2(100) PATH 'TREE_VERSION_NAME',
                    EFFECTIVE_START_DATE DATE PATH 'EFFECTIVE_START_DATE',
                    EFFECTIVE_END_DATE DATE PATH 'EFFECTIVE_END_DATE'
            ) x
        ) src
        ON (etv.TREE_VERSION_NAME = src.TREE_VERSION_NAME)
            WHEN MATCHED THEN
                UPDATE SET
                    etv.EFFECTIVE_START_DATE = src.EFFECTIVE_START_DATE,
                    etv.EFFECTIVE_END_DATE = src.EFFECTIVE_END_DATE
            WHEN NOT MATCHED THEN
                INSERT (
                    etv.TREE_NAME,
                    etv.TREE_CODE,
                    etv.TREE_VERSION_NAME,
                    etv.EFFECTIVE_START_DATE,
                    etv.EFFECTIVE_END_DATE,
                    etv.CURRENT_FLAG,
                    etv.UPLOAD_FLAG
                )
                VALUES (
                    src.TREE_NAME,
                    src.TREE_CODE,
                    src.TREE_VERSION_NAME,
                    src.EFFECTIVE_START_DATE,
                    src.EFFECTIVE_END_DATE,
                    'N',
                    'N'
                );
           
    ELSE
        RAISE e_rest_failed;  -- raising the custom exception
    END IF;

    p_result := l_result;

EXCEPTION
    WHEN e_rest_failed THEN
        p_result := l_result;
        p_error_message := 'REST API call failed.'||p_error_message;
    WHEN OTHERS THEN
        p_result := 'Error';
        p_error_message := 'Unexpected error: ' || SQLERRM;
END load_tree_versions;  

--Check for new values in segment value set and bring in to mapping table
PROCEDURE refresh_mapping_values (
    p_value_set IN VARCHAR2
) IS
    l_map_table VARCHAR2(50);

BEGIN

    CASE p_value_set
    WHEN 'Account KDA_Hovedbok' THEN
        l_map_table := 'MAP_SEG2_ACCOUNT';
    END CASE;

EXCEPTION    
    WHEN CASE_NOT_FOUND THEN
      ROLLBACK;
        APEX_ERROR.ADD_ERROR(
            p_message => 'Value set not found: '||p_value_set,
            p_display_location   => apex_error.c_inline_in_notification
        );

END refresh_mapping_values;

END "ENS_HIERARCHIES_PKG";
/