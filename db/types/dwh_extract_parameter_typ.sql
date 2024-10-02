CREATE OR REPLACE TYPE dwh_extract_parameter_typ AS OBJECT (
    position NUMBER,
    name     VARCHAR2(50),
    value    VARCHAR2(250)
);
/

CREATE OR REPLACE TYPE dwh_extract_parameter_tab AS TABLE OF dwh_extract_parameter_typ;
/