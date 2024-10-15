create or replace package "DWH_EXTRACT_PKG" as
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

/*
--function to check if extract name is valid. Inwoked from WF.
function get_extract_id (
    p_msg_id       in dbo_msg_outbound.msg_id%type,
    p_extract_name in dwh_extract_setup.name%type
) return number;
*/

--create and store outbound message. start DBMS_SCHEDULER JOB. invoked from Manage Extracts screen
procedure start_adhoc_extract(
    p_extract_id    IN  dwh_extract_setup.id%type,
    p_param_json    IN  clob,
    p_msg_id        OUT dbo_msg_outbound.msg_id%type
);

--run extract
procedure run_extract(
    p_msg_id        IN dbo_msg_outbound.msg_id%type,
    p_extract_id    IN dwh_extract_setup.id%type,
    p_parameters    IN dwh_extract_parameter_tab,
    p_result        OUT VARCHAR2, 
    p_error_message OUT VARCHAR2
);

-- function to calculate extract parameter value
FUNCTION calculate_parameter_value(
    p_extract_name    in dwh_extract_setup.name%type,
    p_parameter_name  in varchar2,
    p_calc_attr1      in varchar2 DEFAULT NULL, 
    p_calc_attr2      in varchar2 DEFAULT NULL,
    p_calc_attr3      in varchar2 DEFAULT NULL
) return varchar2;


--This procedure is called from DBMS_SCHEDULER JOBS
PROCEDURE execute_extract_process(
    p_extract_name IN VARCHAR2
);

/*
function is_running (
    p_application_id IN number, 
    p_static_id IN varchar2
) return varchar2;
*/
end "DWH_EXTRACT_PKG";
/