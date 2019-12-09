



##############################
##############################
##############################
##############################
#
#
# Program: T2D_Join_For_Medication_Dates.R
# Version: v0.1.1
# Author: Thomas A. Peterson
#
# Description:
#     Makes a join to get medication records.
#
# Input: 
#     database name: name of main OMOP database
#     sql username: username used to login to sql
#     host_name: name of server hosting the sql database
#     local database name: name of the local database used to store query results
#
##############################
##############################
##############################



options(warn=2)
args = commandArgs(trailingOnly=TRUE)
#args = c("OMOP_DeID", "peterst1", "info-dwsql04dev", "UCWay_DrugPathways");




if(FALSE){

    #spoof command line input for debugging
  options(warn=2)
  args = c("OMOP_DeID", "peterst1", "info-dwsql04dev", "UCWay_DrugPathways");
  

}



main_EHR_db_name = args[1];
mysql_user_name = args[2];
host_name = args[3];
local_db_name = args[4];


suppressWarnings( library(odbc) );

debugit <- TRUE;


mydb <- dbConnect(odbc(),
                  #Driver = "SQL Server",
                  Driver = "ODBC Driver 13 for SQL Server",
                  Server = host_name,
                  Database = main_EHR_db_name,
                  Trusted_Connection="yes",
                  Port = 1433)











#############
#############
############# This section makes the large joins on medications and visits
#############
#############

print("get all patient's relevant medications")
print(Sys.time())
print("")


  #get all patient's relevant medications

query <- paste0("DROP TABLE IF EXISTS ",local_db_name,".dbo.medication_dates_table;");

aaa = dbGetQuery(mydb, query);


  #this commented out query is for a fix if ATC codes don't match up
if(TRUE){
  query <- paste0("SELECT person_id, drug_concept_id, drug_type_concept_id, provider_id,
                      drug_exposure_start_date, drug_exposure_end_date, visit_occurrence_id
                    INTO ",local_db_name,".dbo.medication_dates_table 
                    FROM drug_exposure
                    WHERE 
                      person_id IN (
                        SELECT * FROM ",local_db_name,".dbo.person_ids
                      )
                    AND ( drug_concept_id IN (
                      SELECT r.concept_id_2 FROM concept AS c 
                      LEFT JOIN concept_relationship AS r 
                        ON r.concept_id_1 = c.concept_id OR r.concept_id_2 = c.concept_id
                      WHERE r.relationship_id = 'ATC - RxNorm'
                      AND c.vocabulary_id = 'ATC'
                      AND (c.concept_code LIKE 'A10A%' OR c.concept_code LIKE 'A10B%')
                      )
                    OR drug_concept_id IN (
                      SELECT concept_id_2 from concept_relationship where concept_id_1 IN (
                        SELECT r.concept_id_2 FROM concept AS c 
                        LEFT JOIN concept_relationship AS r 
                          ON r.concept_id_1 = c.concept_id OR r.concept_id_2 = c.concept_id
                        WHERE r.relationship_id = 'ATC - RxNorm'
                        AND c.vocabulary_id = 'ATC'
                        AND (c.concept_code LIKE 'A10A%' OR c.concept_code LIKE 'A10B%')
                      ) )
                    OR drug_concept_id IN (
                      SELECT concept_id FROM concept
                      WHERE ( 
                        concept_name LIKE '%canagliflozin%' OR
                        concept_name LIKE '%dapagliflozin%' OR
                        concept_name LIKE '%empagliflozin%' OR
                        concept_name LIKE '%ertugliflozin%' OR
                        concept_name LIKE '%liraglutide%' OR
                        concept_name LIKE '%albiglutide%' OR
                        concept_name LIKE '%dulaglutide%' OR
                        concept_name LIKE '%semaglutide%' OR
                        concept_name LIKE '%exenatide%' OR
                        concept_name LIKE '%lixisenatide%' OR
                        concept_name LIKE '%repaglinide%' OR
                        concept_name LIKE '%nateglinide%' OR
                        concept_name LIKE '%acarbose%' OR
                        concept_name LIKE '%miglitol%' OR
                        concept_name LIKE '%glyburide%' OR
                        concept_name LIKE '%glimepiride%' OR
                        concept_name LIKE '%glipizide%' OR
                        concept_name LIKE '%chlorpropamide%' OR
                        concept_name LIKE '%tolazamide%' OR
                        concept_name LIKE '%tolbutamide%' OR
                        concept_name LIKE '%sitagliptin%' OR
                        concept_name LIKE '%linagliptin%' OR
                        concept_name LIKE '%saxagliptin%' OR
                        concept_name LIKE '%alogliptin%'
                      )
                    )
                    ) ; ");
}


  #this is the query to use if ATC mapping is working propperly
if(FALSE){
  query <- paste0("SELECT person_id, drug_concept_id, drug_type_concept_id,
                  drug_exposure_start_date, drug_exposure_end_date, visit_occurrence_id
                  INTO ",local_db_name,".dbo.medication_dates_table 
                  FROM drug_exposure
                  WHERE person_id IN (
                    SELECT * FROM ",local_db_name,".dbo.person_ids
                  )
                  AND ( drug_concept_id IN (
                    SELECT r.concept_id_2 FROM concept AS c 
                    LEFT JOIN concept_relationship AS r 
                      ON r.concept_id_1 = c.concept_id OR r.concept_id_2 = c.concept_id
                    WHERE r.relationship_id = 'ATC - RxNorm'
                    AND c.vocabulary_id = 'ATC'
                    AND (c.concept_code LIKE 'A10A%' OR c.concept_code LIKE 'A10B%')
                  )
                  OR drug_concept_id IN (
                    SELECT concept_id_2 from concept_relationship where concept_id_1 IN (
                      SELECT r.concept_id_2 FROM concept AS c 
                      LEFT JOIN concept_relationship AS r 
                        ON r.concept_id_1 = c.concept_id OR r.concept_id_2 = c.concept_id
                      WHERE r.relationship_id = 'ATC - RxNorm'
                      AND c.vocabulary_id = 'ATC'
                      AND (c.concept_code LIKE 'A10A%' OR c.concept_code LIKE 'A10B%')
                    )
                  )
                  
                  ) ; ");

}

aaa = dbGetQuery(mydb, query);


print("get medication names ")
print(Sys.time())
print("")


#get medication names 

query <- paste0("ALTER TABLE ",local_db_name,".dbo.medication_dates_table 
                ADD drug_concept_name Varchar (255) ");
aaa = dbGetQuery(mydb, query);

query <- paste0("UPDATE ",local_db_name,".dbo.medication_dates_table 
                SET drug_concept_name = concept_name
                FROM concept WHERE concept_id = drug_concept_id;");
aaa = dbGetQuery(mydb, query);








print("get drug_type_concept ")
print(Sys.time())
print("")


#get drug_type_concept

query <- paste0("ALTER TABLE ",local_db_name,".dbo.medication_dates_table 
                ADD drug_type_concept_name Varchar (255) ");
aaa = dbGetQuery(mydb, query);

query <- paste0("UPDATE ",local_db_name,".dbo.medication_dates_table 
                SET drug_type_concept_name = concept_name
                FROM concept WHERE concept_id = drug_type_concept_id;");
aaa = dbGetQuery(mydb, query);






print("get visit ")
print(Sys.time())
print("")


query <- paste0("ALTER TABLE ",local_db_name,".dbo.medication_dates_table 
                ADD care_site_id bigint ");
aaa = dbGetQuery(mydb, query);

query <- paste0("ALTER TABLE ",local_db_name,".dbo.medication_dates_table 
                ADD visit_concept_id bigint ");
aaa = dbGetQuery(mydb, query);


query <- paste0("UPDATE M
                SET M.care_site_id = CAST(V.care_site_id AS bigint), 
                    M.visit_concept_id = CAST(V.visit_concept_id as bigint) 
                  FROM ",local_db_name,".dbo.medication_dates_table M 
                JOIN visit_occurrence V 
                  ON V.visit_occurrence_id = M.visit_occurrence_id;");

aaa = dbGetQuery(mydb, query);









print("get care site; location ")
print(Sys.time())
print("")


query <- paste0("ALTER TABLE ",local_db_name,".dbo.medication_dates_table 
                ADD care_site_name Varchar(255) ");
aaa = dbGetQuery(mydb, query);

query <- paste0("ALTER TABLE ",local_db_name,".dbo.medication_dates_table 
                ADD location_id bigint ");
aaa = dbGetQuery(mydb, query);


query <- paste0("UPDATE M
                SET M.care_site_name = CAST(C.care_site_name AS Varchar ), 
                    M.location_id = CAST(C.location_id AS bigint) 
                FROM ",local_db_name,".dbo.medication_dates_table M 
                JOIN care_site C 
                  ON C.care_site_id = M.care_site_id;");

aaa = dbGetQuery(mydb, query);






print("get visit type ")
print(Sys.time())
print("")



query <- paste0("ALTER TABLE ",local_db_name,".dbo.medication_dates_table 
                ADD visit_type Varchar(255) ");
aaa = dbGetQuery(mydb, query);



query <- paste0("UPDATE M
                SET M.visit_type = CAST(C.concept_name AS Varchar) 
                FROM ",local_db_name,".dbo.medication_dates_table M 
                JOIN concept C 
                ON C.concept_id = M.visit_concept_id;");

aaa = dbGetQuery(mydb, query);







print("get site ")
print(Sys.time())
print("")



query <- paste0("ALTER TABLE ",local_db_name,".dbo.medication_dates_table 
                ADD site Varchar(255) ");
aaa = dbGetQuery(mydb, query);



query <- paste0("UPDATE M
                SET M.site = CAST(L.location_source_value AS Varchar) 
                FROM ",local_db_name,".dbo.medication_dates_table M 
                JOIN location L 
                ON L.location_id = M.location_id;");

aaa = dbGetQuery(mydb, query);










#############
#############
############# This section makes the joins for provider (from visit table)
#############
#############




print("get providers ")
print(Sys.time())
print("")




query <- paste0("UPDATE M
                SET M.provider_id = CO.provider_id
                FROM ",local_db_name,".dbo.medication_dates_table M
                JOIN visit_occurrence CO
                ON CO.visit_occurrence_id = M.visit_occurrence_id;");

aaa = dbGetQuery(mydb, query);








