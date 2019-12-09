



##############################
##############################
##############################
##############################
#
#
# Program: T2D_Join_For_Factors.R
# Version: v0.1.1
# Author: Thomas A. Petersons
#
# Description:
#     Makes a join on the index dates table to find factors to use in analysis
#
# Input: 
#     source database name (e.g., OMOP_DeID)
#     sql username (e.g., peterst1)
#     host name (e.g., info-dwsql04dev)
#     database name (e.g., UCWay_DrugPathways)
#     table name (e.g., medication_index_dates)
#
##############################
##############################
##############################



options(warn=2)
args = commandArgs(trailingOnly=TRUE)
#args = c("OMOP_DeID", "peterst1", "info-dwsql04dev", "UCWay_DrugPathways", "medication_index_dates");




if(FALSE){
  
  #spoof command line input for debugging
  options(warn=2)
  args = c("OMOP_DeID", "peterst1", "info-dwsql04dev", "UCWay_DrugPathways", "medication_index_dates");
  
  
}



main_EHR_db_name = args[1];
mysql_user_name = args[2];
host_name = args[3];
local_db_name = args[4];
db_to_join = args[5];


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
############# This section makes the joins for patient table factors
#############
#############

print("patient table factors")
print(Sys.time())
print("")

#race
races_to_check <- c("Asian", "Black or African American", "White")

for(race in races_to_check){
  print(race)
  race_underscores <- gsub(" ", "_", race);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD ",race_underscores," bit  ");
  aaa = dbGetQuery(mydb, query);
  
  query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                  SET ",race_underscores," = 0;")
  aaa = dbGetQuery(mydb, query);
  
  
  
  
  query <- paste0("UPDATE M
                  SET M.",race_underscores," = 1
                  FROM ",local_db_name,".dbo.",db_to_join," M 
                  JOIN person P 
                  ON P.person_id = M.person_id
                  JOIN concept C
                  ON P.race_concept_id = C.concept_id
                  WHERE C.concept_name = '",race,"'
                  ;");
  
  aaa = dbGetQuery(mydb, query);
  
  
}


#ethnicity
eth_to_check <- c("Hispanic or Latino")

for(eth in eth_to_check){
  print(eth)
  eth_underscores <- gsub(" ", "_", eth);
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD ",eth_underscores," bit  ");
  aaa = dbGetQuery(mydb, query);
  
  query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                  SET ",eth_underscores," = 0;")
  aaa = dbGetQuery(mydb, query);
  
  
  
  
  query <- paste0("UPDATE M
                  SET M.",eth_underscores," = 1
                  FROM ",local_db_name,".dbo.",db_to_join," M 
                  JOIN person P 
                  ON P.person_id = M.person_id
                  JOIN concept C
                  ON P.ethnicity_concept_id = C.concept_id
                  WHERE C.concept_name = '",eth,"'
                  ;");
  
  aaa = dbGetQuery(mydb, query);
  
  
}





#find patient age

query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                ADD Patient_Age int  ");
aaa = dbGetQuery(mydb, query);


query <- paste0("UPDATE M
                SET M.Patient_Age = (YEAR(GetDate()) - P.year_of_birth )
                FROM ",local_db_name,".dbo.",db_to_join," M 
                JOIN person P 
                ON P.person_id = M.person_id
                ;");
aaa = dbGetQuery(mydb, query);












#############
############# This section makes CPT codes BEFORE the day of surgery
#############

codes <- list()


codes[["Spinal_Fusion"]] <- c(
  "22551",'22554', #cervical
  '22590','22600','22558','22612', '22630','22633', #lumbar
  '22610', '22556' #thoracic
)



codes[["Heart_And_Pericardium_Surgery"]] <- c('3301%', '3302%', '3303%', '3304%',
                                              '3305%', '3306%', '3307%', '3308%', '3309%',
                                              '331%', '332%', '333%', '334%', '335%',
                                              '336%', '337%', '338%', '339%'
)

codes[["Vein_And_Artery_Surgery"]] <- c('34%', '35%', '36%', '370%','371%','372%',
                                        '373%','374%','375%','376%', '377%'
)

codes[["Hip_Arthroplasty"]] <- c('27130', '27132', '27134', '27137', '27138', '27125')
codes[["Knee_Arthroplasty"]] <- c('27445', '27447', '27486', '27487', '27446')
codes[["Shoulder_Elbow_Arthroplasty"]] <- c('24360', '24361', '24362', '24363', '24370', '24371',  #elbow
                                            '23470', '23472', '23473', '23474') #shoulder

codes[["Occular_Surgery"]] <- c('6509%', '651%', '652%', '653%', '654%', '655%', '656%', '657%', 
                                '658%', '659%', '66%', '67%', 
                                '681%', '682%', '683%', '684%', '685%', '686%', '687%', '688%'
)






for(ccc in ls(codes)){
  factor_name <- paste0("CPT_History_", ccc)
  code_list <- codes[[ccc]]
  
  print(factor_name)
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  DROP COLUMN IF EXISTS ",factor_name," ");
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD ",factor_name," bit ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                  SET ",factor_name," = 0;")
  aaa = dbGetQuery(mydb, query);
  
  
  for(this_code in code_list){
    
    
    
    query <- paste0("UPDATE M
                    SET M.",factor_name," = 1
                    FROM ",local_db_name,".dbo.",db_to_join," M 
                    JOIN procedure_occurrence PO 
                    ON PO.person_id = M.person_id
                    JOIN concept C
                    ON PO.procedure_concept_id = C.concept_id
                    WHERE PO.procedure_date <= M.index_date
                    AND C.concept_code LIKE '",this_code,"';");
    
    aaa = dbGetQuery(mydb, query);
    
  }
}















#############
#############
############# This section makes the joins for ICD10 codes
#############
#############

print("ICD10 code factors")
print(Sys.time())
print("")




codes <- list()
codes[["Cancer"]] <- "C%|D0%|D1%|D2%|D3%|D4";
codes[["Problems_Housing_Economic"]] <- "Z59%";


codes[["Asthma"]] <- "J45%";
codes[["COPD"]] <- "J44%";
codes[["Osteoporosis"]] <- "M81%|M80%";
codes[["GERD"]] <- "K21.9%";
codes[["Insomnia"]] <- "G47.0%";
codes[["Sleep_Apnea"]] <- "G47.3%";
codes[["Rheumatic_Disease"]] <- "M05%|M06%|M79.0%";
codes[["Depression"]] <- "F32%|F33%";
codes[["Hypertension"]] <- "I10%";
codes[["CKD"]] <- "I12%|D63.1%|N17%|N18%|N19%|E11.2%";
codes[["Liver_Damage"]] <- "K70%|K71%|K72%|K73%|K74%|K75%|K76%|K77%";
codes[["Heart_Attack"]] <- "I21%|I22%|I23.0%";
codes[["Heart_Failure"]] <- "I50%";
codes[["PAD"]] <- "I73%";
codes[["Heart_Block"]] <- "I44%";
codes[["Stroke"]] <- "I63%";
codes[["Cardiomyopathy"]] <- "I42%|I43%";
codes[["Arrhythmias"]] <- "I48%|I49%|R00.1%|R94.31%";
codes[["Myocarditis"]] <- "I51%";
codes[["MACE"]] <- "I51%|I48%|I49%|R00.1%|R94.31%|I42%|I43%|I63%|I44%|I73%|I50%|I21%|I22%|I23.0%";
codes[["Diabetes_Mellitus_Family"]] <- "Z83.3%";
codes[["T2D_With_Ophtalmic_Complication"]] <- "E11.3%|H35.109%";
codes[["T2D_With_Hyperosmolarity_Complication"]] <- "E11.0%";
codes[["T2D_With_Neurological_Complication"]] <- "E11.4%";
codes[["T2D_With_Circulatory_Complication"]] <- "E11.5%";
codes[["Cannabis_Related_Disorders"]] <- "F12%";
codes[["Opioid_Related_Disorders"]] <- "F11%";
codes[["Cocaine_Related_Disorders"]] <- "F14%";
codes[["Hallucinogen_Related_Disorders"]] <- "F16%";
codes[["Other_Psychoactive_Substance_Related_Disorders"]] <- "F19%";


codes <- data.frame(codes)


for(ccc in 1:ncol(codes)){
  factor_name <- as.character(colnames(codes)[ccc])
  factor_codes <- strsplit(as.character(codes[1,ccc]), "\\|")[[1]]
  
  print(factor_name)
  
  ###########
  ########### Diagnosis History
  ###########
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD ",factor_name,"_History bit ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                  SET ",factor_name,"_History = 0;")
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("UPDATE M
                  SET M.",factor_name,"_History = 1
                  FROM ",local_db_name,".dbo.",db_to_join," M 
                  JOIN condition_occurrence CO 
                  ON CO.person_id = M.person_id
                  JOIN concept C
                  ON CO.condition_concept_id = C.concept_id
                  WHERE CO.condition_start_date <= M.index_date
                  AND ( C.concept_code LIKE '", paste(factor_codes, collapse="' OR C.concept_code LIKE '") ,"' );");
  
  aaa = dbGetQuery(mydb, query);
  
  
  
  
  ###########
  ########### Diagnosis Future development
  ###########
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD Future_Development_of_",factor_name," bit ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                  SET Future_Development_of_",factor_name," = 0;")
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("UPDATE M
                  SET M.Future_Development_of_",factor_name," = 1
                  FROM ",local_db_name,".dbo.",db_to_join," M 
                  JOIN condition_occurrence CO 
                  ON CO.person_id = M.person_id
                  JOIN concept C
                  ON CO.condition_concept_id = C.concept_id
                  WHERE CO.condition_start_date > M.index_date
                  AND ( C.concept_code LIKE '", paste(factor_codes, collapse="' OR C.concept_code LIKE '") ,"' );");
  
  aaa = dbGetQuery(mydb, query);
  
  
  
  
  
  ###########
  ########### Diagnosis Date_Of_Next_event
  ###########
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD Date_Of_Next_",factor_name," Date ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                  SET Date_Of_Next_",factor_name," = NULL;")
  aaa = dbGetQuery(mydb, query);
  
  
  
  query <- paste0("UPDATE M
                    SET M.Date_Of_Next_",factor_name," = A.",factor_name,"_Next_Date
                    FROM ",local_db_name,".dbo.",db_to_join," M,
                      (SELECT M2.person_id, M2.index_date, CO.condition_start_date AS ",factor_name,"_Next_Date, 
                              row_number() OVER(PARTITION BY M2.person_id, M2.index_date ORDER BY CO.condition_start_date) as rn
                        FROM ",local_db_name,".dbo.",db_to_join," M2
                        JOIN condition_occurrence CO
                          ON CO.person_id = M2.person_id
                        LEFT JOIN concept c1 
                          ON c1.concept_id = CO.condition_concept_id
                        WHERE CO.condition_start_date > M2.index_date
                          AND ( c1.concept_code LIKE '", paste(factor_codes, collapse="' OR c1.concept_code LIKE '") ,"' )
                      ) A
                  WHERE M.person_id = A.person_id 
                  AND M.index_date = A.index_date 
                  AND A.rn = 1;");
  
  aaa = dbGetQuery(mydb, query);
  
  
  
  
}























#############
#############
############# This section makes the joins for labs and vitals
#############
#############

print("labs and vitals")
print(Sys.time())
print("")





types_of_measurements <- new.env(hash=TRUE)

#name <- #c(pattern, Time_To_Decrease threshold)
types_of_measurements[["HbA1c"]] <- c("a1c", "percent");
types_of_measurements[["BMI"]] <- c("body mass index", "");
types_of_measurements[["Height"]] <- c("Body height", "");
types_of_measurements[["Weight"]] <- c("Body weight", "");
types_of_measurements[["LDL"]] <- c("Cholesterol in LDL%volume", "milligram per deciliter");
types_of_measurements[["HDL"]] <- c("Cholesterol in HDL%volume", "milligram per deciliter");
types_of_measurements[["eGFR"]] <- c("egfr", "");
types_of_measurements[["Triglyceride"]] <- c("Triglyceride", "milligram per deciliter");
types_of_measurements[["Systolic_BP"]] <- c("BP systolic", "");
types_of_measurements[["Calcium"]] <- c("Calcium serum%plasma serum%plasma", "milligram per deciliter");
types_of_measurements[["Albumin"]] <- c("Albumin  serum%plasma", "gram per deciliter");
types_of_measurements[["Alkaline_Phosphatase"]] <- c("Alkaline phosphatase serum%plasma", "milligram per deciliter");
types_of_measurements[["Bilirubin"]] <- c("Total Bilirubin serum%plasma", "milligram per deciliter");
types_of_measurements[["Creatinine"]] <- c("Creatinine serum%plasma", "milligram per deciliter");
types_of_measurements[["Alanine_Aminotransferase"]] <- c("alanine%aminotransferase%serum%plasma", "milligram per deciliter");
types_of_measurements[["Urea_Nitrogen"]] <- c("Urea%nitrogen%serum%plasma", "milligram per deciliter");
types_of_measurements[["Carbon_Dioxide"]] <- c("Carbon%dioxide%serum%plasma", "millimole per liter");
types_of_measurements[["Sodium"]] <- c("Sodium%serum%plasma", "millimole per liter");
types_of_measurements[["Chloride"]] <- c("Chloride%serum%plasma", "millimole per liter");
types_of_measurements[["Anion_Gap"]] <- c("Anion%gap%serum%plasma", "millimole per liter");
types_of_measurements[["Aspartate_Aminotransferase"]] <- c("Aspartate%aminotransferase%serum%plasma", "milligram per deciliter");
types_of_measurements[["Vitamin_B12"]] <- c("Cobalamin%B12%serum%plasma", "picogram per milliliter");
types_of_measurements[["Magnesium"]] <- c("Magnesium%serum%plasma", "milligram per deciliter");
types_of_measurements[["Thyrotropin"]] <- c("Thyrotropin%serum%plasma", "milligram per deciliter");




for(m in ls(envir=types_of_measurements)){
  
  factor_pattern <- types_of_measurements[[m]][1]
  factor_units <- types_of_measurements[[m]][2]
  
  print(m)
  
  
    #remove columns if they're already there
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  DROP COLUMN IF EXISTS ",m,"_Pre_Index ");
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  DROP COLUMN IF EXISTS  ",m,"_Pre_Index_Date  ");
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  DROP COLUMN IF EXISTS  ",m,"_Pre_Index_Unit  ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  DROP COLUMN IF EXISTS  ",m,"_Post_Index  ");
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  DROP COLUMN IF EXISTS  ",m,"_Post_Index_Date  ");
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  DROP COLUMN IF EXISTS  ",m,"_Post_Index_Unit  ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  DROP COLUMN IF EXISTS  First_",m,"  ");
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  DROP COLUMN IF EXISTS  First_",m,"_Date ");
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  DROP COLUMN IF EXISTS  First_",m,"_Unit ");
  aaa = dbGetQuery(mydb, query);
  
  
  
    #set up columns
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD ",m,"_Pre_Index float ");
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD ",m,"_Pre_Index_Date Date ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD ",m,"_Pre_Index_Unit Varchar (100) ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD ",m,"_Post_Index float ");
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD ",m,"_Post_Index_Date Date ");
  aaa = dbGetQuery(mydb, query);
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD ",m,"_Post_Index_Unit Varchar(100) ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD First_",m," float ");
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD First_",m,"_Date Date ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                  ADD First_",m,"_Unit Varchar(100) ");
  aaa = dbGetQuery(mydb, query);
  
  
  
    #makes pre-date
  query <- paste0("UPDATE M
                  SET M.",m,"_Pre_Index = A.",m,"_Pre_Index,
                      M.",m,"_Pre_Index_Date = A.",m,"_Pre_Index_Date, 
                      M.",m,"_Pre_Index_Unit = A.",m,"_Pre_Index_Unit 
                  FROM ",local_db_name,".dbo.",db_to_join," M,
                    (SELECT M2.person_id, M2.index_date, MT.measurement_date AS ",m,"_Pre_Index_Date, 
                            MT.value_as_number AS ",m,"_Pre_Index, 
                            C.concept_name AS ",m,"_Pre_Index_Unit, 
                            row_number() OVER(PARTITION BY M2.person_id, M2.index_date ORDER BY MT.measurement_date DESC) as rn
                      FROM ",local_db_name,".dbo.",db_to_join," M2
                      JOIN measurement MT
                        ON MT.person_id = M2.person_id
                        AND MT.measurement_date <= DATEADD(day, 7, M2.index_date)
                        AND MT.measurement_concept_id IN (
                          SELECT concept_id FROM concept 
                          WHERE concept_name LIKE '%", factor_pattern,"%' 
                          AND vocabulary_id = 'LOINC'
                        )
                      LEFT JOIN concept C
                        ON C.concept_id = MT.unit_concept_id
                    ) A
                  WHERE M.person_id = A.person_id 
                  AND M.index_date = A.index_date 
                  AND A.rn = 1;");
  
  aaa = dbGetQuery(mydb, query);
  
  
  
  
  
    #makes post-date + 60 days
  query <- paste0("UPDATE M
                  SET M.",m,"_Post_Index = A.",m,"_Post_Index,
                      M.",m,"_Post_Index_Date = A.",m,"_Post_Index_Date,
                      M.",m,"_Post_Index_Unit = A.",m,"_Post_Index_Unit 
                  FROM ",local_db_name,".dbo.",db_to_join," M,
                      (SELECT M2.person_id, M2.index_date, MT.measurement_date AS ",m,"_Post_Index_Date, 
                              MT.value_as_number AS ",m,"_Post_Index, 
                              C.concept_name AS ",m,"_Post_Index_Unit, 
                              row_number() OVER(PARTITION BY M2.person_id, M2.index_date ORDER BY MT.measurement_date ASC) as rn
                        FROM ",local_db_name,".dbo.",db_to_join," M2
                        JOIN measurement MT
                        ON MT.person_id = M2.person_id
                          AND MT.measurement_date >= DATEADD(day, 60, M2.index_date)
                          AND MT.measurement_concept_id IN (
                            SELECT concept_id FROM concept 
                            WHERE concept_name LIKE '%", factor_pattern,"%' 
                            AND vocabulary_id = 'LOINC'
                          )
                        LEFT JOIN concept C
                        ON C.concept_id = MT.unit_concept_id
                      ) A
                  WHERE M.person_id = A.person_id 
                  AND M.index_date = A.index_date 
                  AND A.rn = 1;");
  aaa = dbGetQuery(mydb, query);
  
  
  
  
  
  
  #makes first measurement
  query <- paste0("UPDATE M
                    SET M.First_",m," = A.First_",m,",
                      M.First_",m,"_Date = A.First_",m,"_Date, 
                      M.First_",m,"_Unit = A.First_",m,"_Unit
                    FROM ",local_db_name,".dbo.",db_to_join," M,
                      (SELECT M2.person_id, M2.index_date, MT.measurement_date AS First_",m,"_Date, 
                          MT.value_as_number AS First_",m,", 
                          C.concept_name AS First_",m,"_Unit, 
                          row_number() OVER(PARTITION BY M2.person_id, M2.index_date ORDER BY MT.measurement_date ASC) as rn
                        FROM ",local_db_name,".dbo.",db_to_join," M2
                        JOIN measurement MT
                        ON MT.person_id = M2.person_id
                        AND MT.measurement_concept_id IN (
                          SELECT concept_id FROM concept 
                          WHERE concept_name LIKE '%", factor_pattern,"%' 
                          AND vocabulary_id = 'LOINC'
                        )
                        LEFT JOIN concept C
                        ON C.concept_id = MT.unit_concept_id
                      ) A
                    WHERE M.person_id = A.person_id 
                    AND M.index_date = A.index_date 
                    AND A.rn = 1;");
  aaa = dbGetQuery(mydb, query);
  
  
  
}









#############
#############
############# This section makes the joins for concurrent medications
#############
#############

print("concurrent medications")
print(Sys.time())
print("")



codes <- list()


codes[["Antihyperglycemic"]] <- "A10A%|A10B%";
codes[["Statins"]] <- "C10AA%|C10B%";
codes[["Antihypertensive"]] <- "C02%";
codes[["Diuretic"]] <- "C03%";
codes[["Vasodilator"]] <- "C04%";
codes[["Vasoprotective"]] <- "C05%";
codes[["Beta_Blocker"]] <- "C07%";
codes[["Calcium_Channel_Blocker"]] <- "C08%";
codes[["Cardiac_Therapy"]] <- "C01%";
codes[["Dermatological"]] <- "D%";
codes[["Pituitary_or_Hypothalamic_Hormones"]] <- "H01%";
codes[["Corticosteroids_Systemic_Use"]] <- "H02%";
codes[["Thyroid_Hormones"]] <- "H03%";
codes[["Pancreatic_Hormones"]] <- "H04%";
codes[["Calcium_Homeostasis_Hormones"]] <- "H05%";
codes[["Antiinflammatory_or_Antirheumatic"]] <- "M01%";
codes[["Antigout"]] <- "M04%";
codes[["Antiobstructive_Airway"]] <- "R03%";
codes[["Opioids"]] <- "N02AA%";



codes <- data.frame(codes)


for(ccc in 1:ncol(codes)){
  
  factor_name <- as.character(colnames(codes)[ccc])
  factor_codes <- strsplit(as.character(codes[1,ccc]), "\\|")[[1]]
  
  print(factor_name)
  
  query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join,"
                  ADD Concurrent_",factor_name," bit ");
  aaa = dbGetQuery(mydb, query);
  
  
  
  query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join,"
                  SET Concurrent_",factor_name," = 0;")
  aaa = dbGetQuery(mydb, query);
  
  
  query <- paste0("UPDATE M
                  SET M.Concurrent_",factor_name," = 1
                  FROM ",local_db_name,".dbo.",db_to_join," M 
                  LEFT JOIN drug_exposure D 
                  ON D.person_id = M.person_id
                  WHERE D.drug_exposure_start_date <= M.index_date
                  AND (D.drug_exposure_end_date > M.index_date OR D.drug_exposure_end_date IS NULL)
                  AND D.drug_concept_id IN (
                  SELECT r.concept_id_2 FROM concept AS c 
                  LEFT JOIN concept_relationship AS r 
                  ON r.concept_id_1 = c.concept_id OR r.concept_id_2 = c.concept_id
                  WHERE r.relationship_id = 'ATC - RxNorm'
                  AND c.vocabulary_id = 'ATC'
                  AND ( C.concept_code LIKE '", paste(factor_codes, collapse="' OR C.concept_code LIKE '") ,"'
                  ) ) ;");
  
  aaa = dbGetQuery(mydb, query);
  
  
  
  
}






#############
#############
############# This section makes the joins for death date
#############
#############




query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                ADD Date_Of_Death Date ");
aaa = dbGetQuery(mydb, query);


query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                SET Date_Of_Death = NULL;")
aaa = dbGetQuery(mydb, query);






query <- paste0("UPDATE M
                SET M.Date_Of_Death = D.death_date
                FROM ",local_db_name,".dbo.",db_to_join," M
                LEFT JOIN death D
                ON M.person_id = D.person_id;");

aaa = dbGetQuery(mydb, query);










#############
#############
############# This section makes the joins for most recent visit, diagnosis
#############
#############






query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                ADD Date_Of_Most_Recent_Condition_Occurrence Date ");
aaa = dbGetQuery(mydb, query);


query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                SET Date_Of_Most_Recent_Condition_Occurrence = NULL;")
aaa = dbGetQuery(mydb, query);



query <- paste0("UPDATE M
                SET M.Date_Of_Most_Recent_Condition_Occurrence = A.Last_Date
                FROM ",local_db_name,".dbo.",db_to_join," M,
                (SELECT M2.person_id, CO.condition_start_date AS Last_Date, 
                row_number() OVER(PARTITION BY M2.person_id, M2.index_date ORDER BY CO.condition_start_date DESC) as rn
                FROM ",local_db_name,".dbo.",db_to_join," M2
                JOIN condition_occurrence CO
                ON CO.person_id = M2.person_id) A
                WHERE M.person_id = A.person_id 
                AND A.rn = 1;");

aaa = dbGetQuery(mydb, query);











query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                ADD Date_Of_Most_Recent_Visit Date ");
aaa = dbGetQuery(mydb, query);


query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                SET Date_Of_Most_Recent_Visit = NULL;")
aaa = dbGetQuery(mydb, query);



query <- paste0("UPDATE M
                SET M.Date_Of_Most_Recent_Visit = A.Last_Date
                FROM ",local_db_name,".dbo.",db_to_join," M,
                (SELECT M2.person_id, CO.visit_start_date AS Last_Date, 
                row_number() OVER(PARTITION BY M2.person_id, M2.index_date ORDER BY CO.visit_start_date DESC) as rn
                FROM ",local_db_name,".dbo.",db_to_join," M2
                JOIN visit_occurrence CO
                ON CO.person_id = M2.person_id) A
                WHERE M.person_id = A.person_id 
                AND A.rn = 1;");

aaa = dbGetQuery(mydb, query);



















query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                ADD Current_Smoker bit  ");
aaa = dbGetQuery(mydb, query);

query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                SET Current_Smoker = 0;")
aaa = dbGetQuery(mydb, query);



query <- paste0("UPDATE M
                SET M.Current_Smoker = 1
                FROM ",local_db_name,".dbo.",db_to_join," M 
                JOIN observation O 
                ON O.person_id = M.person_id
                JOIN concept C
                ON O.value_as_concept_id = C.concept_id
                WHERE O.observation_date <= M.index_date
                AND ( concept_name like '%smoker%'
                      AND concept_name NOT LIKE '%non%'
                      AND concept_name NOT LIKE '%Ex-%'
                );");

aaa = dbGetQuery(mydb, query);








query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                ADD Former_Smoker bit  ");
aaa = dbGetQuery(mydb, query);

query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                SET Former_Smoker = 0;")
aaa = dbGetQuery(mydb, query);



query <- paste0("UPDATE M
                SET M.Former_Smoker = 1
                FROM ",local_db_name,".dbo.",db_to_join," M 
                JOIN observation O 
                ON O.person_id = M.person_id
                JOIN concept C
                ON O.value_as_concept_id = C.concept_id
                WHERE O.observation_date <= M.index_date
                AND ( concept_name like '%smoker%'
                      AND concept_name NOT LIKE '%non%'
                      AND concept_name LIKE '%Ex-%'
                );");

aaa = dbGetQuery(mydb, query);












query <- paste0("ALTER TABLE ",local_db_name,".dbo.",db_to_join," 
                ADD Sex_Female bit  ");
aaa = dbGetQuery(mydb, query);

query <- paste0("UPDATE ",local_db_name,".dbo.",db_to_join," 
                SET Sex_Female = 0;")
aaa = dbGetQuery(mydb, query);



query <- paste0("UPDATE M
                SET Sex_Female = 1
                FROM ",local_db_name,".dbo.",db_to_join," M 
                JOIN person P
                ON P.person_id = M.person_id
                JOIN concept C
                ON P.gender_concept_id = C.concept_id
                WHERE concept_name like '%female%';");

aaa = dbGetQuery(mydb, query);






