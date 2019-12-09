



##############################
##############################
############################## 
##############################
#
#
# Program: T2D_Find_Patients.R
# Version: v0.1.1
# Author: Thomas A. Peterson
#
# Description:
#     Uses include and exclude criteria to identify the patient cohort
#     makes a tables called "person_ids"
#
# Input: 
#     source database name (e.g., OMOP_DeID)
#     sql username (e.g., peterst1)
#     host name (e.g., info-dwsql04dev)
#     database name (e.g., UCWay_DrugPathways)
#
##############################
##############################
##############################



options(warn=2)
args = commandArgs(trailingOnly=TRUE)




if(FALSE){

    #spoof command line input for debugging
  options(warn=2)
  args = c("OMOP_DeID", "peterst1", "info-dwsql04dev", "UCWay_DrugPathways");
  #args = c("OMOP", "peterst1", "info-dwsql04dev");
  #args = c("OHDSI_20180214", "petersont");
  #info-dwsql04dev


}

suppressWarnings( library(odbc) );
suppressWarnings( library(ggplot2) );
suppressWarnings( library(foreach) );
suppressWarnings( library(doParallel) );
suppressWarnings( library(doSNOW) );
suppressWarnings( library(rstudioapi) );


debugit <- TRUE;



main_EHR_db_name = args[1];
mysql_user_name = args[2];
host_name = args[3];
local_db_name = args[4];


#mydb = dbConnect(MySQL(), user=mysql_user_name, dbname=main_EHR_db_name, host=host_name)

#mydb <- dbConnect(odbc::odbc(), main_EHR_db_name)

mydb <- dbConnect(odbc(),
                #Driver = "SQL Server",
                Driver = "ODBC Driver 13 for SQL Server",
                Server = host_name,
                Database = main_EHR_db_name,
                Trusted_Connection="yes",
                Port = 1433)

####################
####################
####################
####################

if(debugit){ print("Guide: Querying for lab measurement inclusion criteria"); }




query = paste0("SELECT DISTINCT person_id FROM measurement m 
	              LEFT JOIN concept c 
                  ON m.measurement_concept_id = c.concept_id
        				WHERE (
                  m.value_as_number > 5.7 
          				AND c.concept_name LIKE '%a1c%'
                  AND vocabulary_id = 'LOINC'
                )
                AND (
                  m.unit_concept_id IS NULL 
                  OR m.unit_concept_id = ''
                  OR m.unit_concept_id IN (
                    SELECT concept_id from concept
                    WHERE concept_class_id = 'Unit'
                    AND (
                      concept_name = 'percent'
                      OR concept_name = 'No matching concept'
                      OR concept_name IS NULL
                    )
                  )
                )
                
               
               ;");


measurement_include = dbGetQuery(mydb, query);




if(debugit){ print("dim(measurement_include)"); }
if(debugit){ print(dim(measurement_include)); }



#for testing# query = paste0("SELECT person_id FROM condition_occurrence limit 1000;")


####################
####################
####################
####################

if(debugit){ print("Guide: Querying for ICD10 inclusion criteria"); }


query <- paste0("SELECT DISTINCT person_id  FROM condition_occurrence co
 LEFT JOIN concept c
 ON c.concept_id = co.condition_concept_id 
 WHERE concept_code LIKE 'E11%'
 OR concept_code LIKE 'R73%';");
    
icd10_include = dbGetQuery(mydb, query);


if(debugit){ print("dim(icd10_include)"); }
if(debugit){ print(dim(icd10_include)); }







####################
####################
####################
####################



if(debugit){ print("Guide: Querying for ICD10 exclusion criteria"); }


query <- paste0("SELECT DISTINCT(person_id) FROM condition_occurrence co
 LEFT JOIN concept c
 ON c.concept_id = co.condition_concept_id 
 WHERE concept_code LIKE 'O%' 
 OR concept_code LIKE 'Z3%' 
 OR concept_code LIKE 'E10%';");
    
icd10_exclude = dbGetQuery(mydb, query);



if(debugit){ print("dim(icd10_exclude)"); }
if(debugit){ print(dim(icd10_exclude)); }




####################
####################
####################
####################



if(debugit){ print("Guide: Querying for medication order criteria"); }


  #this commented out query is for a fix if ATC codes don't match up
if(TRUE){
  query <- "SELECT DISTINCT person_id FROM drug_exposure WHERE 
            ( drug_concept_id IN (
                  SELECT r.concept_id_2 FROM concept AS c 
                    LEFT JOIN concept_relationship AS r 
                      ON r.concept_id_1 = c.concept_id OR r.concept_id_2 = c.concept_id
                    WHERE r.relationship_id = 'ATC - RxNorm'
                    AND c.vocabulary_id = 'ATC'
                    AND (c.concept_code LIKE 'A10A%' OR c.concept_code LIKE 'A10B%')
              )
              OR drug_concept_id IN (
                  SELECT concept_id_2 FROM concept_relationship WHERE concept_id_1 IN (
                    SELECT r.concept_id_2 FROM concept AS c 
                    LEFT JOIN concept_relationship AS r 
                      ON r.concept_id_1 = c.concept_id OR r.concept_id_2 = c.concept_id
                    WHERE r.relationship_id = 'ATC - RxNorm'
                    AND c.vocabulary_id = 'ATC'
                    AND (c.concept_code LIKE 'A10A%' OR c.concept_code LIKE 'A10B%')
                  )
              )
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
            )
  ;";
}  

  #this is the query to use if ATC mapping is working propperly
if(FALSE){
  query <- "SELECT DISTINCT person_id FROM drug_exposure WHERE 
              ( drug_concept_id IN (
                SELECT r.concept_id_2 FROM concept AS c 
                  LEFT JOIN concept_relationship AS r 
                    ON r.concept_id_1 = c.concept_id OR r.concept_id_2 = c.concept_id
                  WHERE r.relationship_id = 'ATC - RxNorm'
                  AND c.vocabulary_id = 'ATC'
                  AND (c.concept_code LIKE 'A10A%' OR c.concept_code LIKE 'A10B%')
                )
                OR drug_concept_id IN (
                  SELECT concept_id_2 FROM concept_relationship WHERE concept_id_1 IN (
                    SELECT r.concept_id_2 FROM concept AS c 
                    LEFT JOIN concept_relationship AS r 
                      ON r.concept_id_1 = c.concept_id OR r.concept_id_2 = c.concept_id
                    WHERE r.relationship_id = 'ATC - RxNorm'
                    AND c.vocabulary_id = 'ATC'
                    AND (c.concept_code LIKE 'A10A%' OR c.concept_code LIKE 'A10B%')
                  )
                )
              )
  ;";
}
med_include = dbGetQuery(mydb, query);





if(debugit){ print("dim(med_include)"); }
if(debugit){ print(dim(med_include)); }



final_include <- data.frame(unique(c(icd10_include[,1], measurement_include[,1] )))
colnames(final_include)[1] <- "person_id"


if(debugit){ print("dim(final_include)"); }
if(debugit){ print(dim(final_include)); }

final_include_w_meds <- final_include[(final_include[,1] %in% med_include[,1]),,drop=FALSE]


if(debugit){ print("dim(final_include_w_meds)"); }
if(debugit){ print(dim(final_include_w_meds)); }


final_patients <- final_include_w_meds[!(final_include_w_meds[,1] %in% icd10_exclude[,1]),,drop=FALSE]


if(debugit){ print("dim(final_patients)"); }
if(debugit){ print(dim(final_patients)); }




####################
#################### Upload table
####################



query <- paste0("DROP TABLE IF EXISTS ",local_db_name,".dbo.person_ids;");

aaa = dbGetQuery(mydb, query);


query <- paste0("CREATE TABLE ",local_db_name,".dbo.person_ids ( person_id int NOT NULL );");

aaa = dbGetQuery(mydb, query);


for(iii in 1:nrow(final_patients)){
#for(iii in 1:50){
  #print(iii)
  if(iii == 1 || iii %% 1000 == 0){ print(iii) }
  
  query <- paste0("INSERT INTO ",local_db_name,".dbo.person_ids VALUES (", as.character(final_patients[iii,]),")")
  aaa = dbGetQuery(mydb, query);
  
}







####################
#################### Print final table
####################


#patid_file <- "./Data/T2D_Patients_Final.txt"


#write.csv(final_patients, file=patid_file);









