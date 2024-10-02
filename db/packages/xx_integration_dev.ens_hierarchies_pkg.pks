create or replace PACKAGE "ENS_HIERARCHIES_PKG" as
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


  --Create new upload to ERP. Invoked from 
  PROCEDURE create_erp_upload (
    p_msg_id in dbo_msg_inbound.msg_id%type,
    p_tree_version in varchar2,
    p_wf_instance_id in varchar2,
    p_user in varchar2,
    p_upload_id out number,
    p_status out varchar2,
    p_status_msg out varchar2
  );

  -- Prepare CSV and Zip it before sending to ERP. Invoked form WF.
  PROCEDURE zip_csv_file (
    p_msg_id         in dbo_msg_inbound.msg_id%type,
    p_upload_id      in ens_segment_hierarchy_uploads.id%type,
    p_wf_instance_id in ens_segment_hierarchy_uploads.wf_instance_id%type,
    p_status         out varchar2,
    p_status_msg     out varchar2
  );

  --Takes hierarchies payload (CSV) from dbo_msg_inbound, converts to base64 and uploads to UCM. Invoked from WF. 
PROCEDURE upload_file_to_ucm (
    p_msg_id     in dbo_msg_inbound.msg_id%type,
    p_upload_id  in ens_segment_hierarchy_uploads.id%type
);

--Start ERP load process to import data from the uploaded zip file. Invoked from WF.
PROCEDURE submit_erp_interface_loader(
    p_msg_id      in  dbo_msg_inbound.msg_id%type,
    p_upload_id   in  ens_segment_hierarchy_uploads.id%type
);

--Process ERP ESS Job CallBack message. Invoked from dbo_erp_utils_pkg.store_erp_callback
PROCEDURE process_erp_callback(
    p_msg_id      in dbo_callback_msg.msg_id%type,
    p_callback_id in dbo_callback_msg.callback_id%type
);

--final validations and set completion status. Invoked from WF.
PROCEDURE set_final_status (
    p_msg_id      in  dbo_msg_inbound.msg_id%type,
    p_upload_id   in  ens_segment_hierarchy_uploads.id%type
);

/*
Procedure to add new value in segment hierarchy. 
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
);

-- Function to check if a value is the segment hierarchy. Returns Y
-- if the value is placed in the hierarchy, N otherwise.
FUNCTION is_value_in_hierarchy(
    p_data_source    IN ens_gl_segment_hier_int.x_data_source%type,
    p_value_set_name IN ens_gl_coa_info.value_set_name%type,
    p_value          IN VARCHAR2
) RETURN VARCHAR2;

--Refresh tree version data from Orafin. If tree version exists then update only effective dates, 
--if version doesnt exist then inser new row by default setting current=N, upload=N 
PROCEDURE load_tree_versions(p_result OUT VARCHAR2, p_error_message OUT VARCHAR2) ;

end "ENS_HIERARCHIES_PKG";
/