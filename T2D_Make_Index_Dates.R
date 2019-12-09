



##############################
##############################
##############################
##############################
#
#
# Program: T2D_Make_Index_Dates.R
# Version: v0.1.1
# Author: Thomas A. Peterson
#
# Description:
#     Find dates where a provider from one of our sites prescribes a new medication
#
# Input: 
#     database name: name of main OMOP database
#     sql username: username used to login to sql
#     host_name: name of server hosting the sql database
#     local database name: name of the local database used to store query results
#
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


num_cores = 3;


suppressWarnings( library(odbc) );
suppressWarnings( library(ggplot2) );
suppressWarnings( library(foreach) );
suppressWarnings( library(doParallel) );
suppressWarnings( library(doSNOW) );
suppressWarnings( library(rstudioapi) );


debugit <- TRUE;


mydb <- dbConnect(odbc(),
                  #Driver = "SQL Server",
                  Driver = "ODBC Driver 13 for SQL Server",
                  Server = host_name,
                  Database = main_EHR_db_name,
                  Trusted_Connection="yes",
                  Port = 1433)




# variable sliding_window_for_dates:
#   makes index dates + / - sliding_window_for_dates days
#   set to 0 to turn off
sliding_window_for_dates = 0;






#####################
#####################
##################### Get index dates from sql; remove inpatient
#####################
#####################


##### get index dates from sql

query <- paste0("SELECT * FROM ",local_db_name,".dbo.medication_dates_table 
                ORDER BY drug_exposure_start_date;");
all_med_dates = dbGetQuery(mydb, query);



#print("TRUNCATINGXXXXXXXXXXXXXXXXXXX")
#all_med_dates <- all_med_dates[1:10000,];

print("dim of all_med_dates:")
print(dim(all_med_dates))


print("table(all_med_dates$visit_type)")
print(table(all_med_dates$visit_type))


##### use only the right visit types

all_med_dates <- all_med_dates[which(all_med_dates$visit_type != "Inpatient Visit"),]



all_med_dates <- all_med_dates[which(all_med_dates$drug_type_concept_name != "Inpatient administration"),]


print("dim of non-inpatient all_med_dates:")
print(dim(all_med_dates))



print("length(unique(all_med_dates$person_id))")
print(length(unique(all_med_dates$person_id)))



print("table(all_med_dates$drug_type_concept_name)")
print(table(all_med_dates$drug_type_concept_name))





#####################
#####################
##################### Build drug concept ID to concept name & ATC mapping
#####################
#####################

if(debugit){ print("Guide: Building drug concept ID to ATC category mapping"); }

query <- "SELECT c1.concept_id, c1.concept_name, c1.concept_code, c1.concept_class_id,
  r.concept_id_1, r.concept_id_2, r.relationship_id,
  c2.concept_id AS 'c2.concept_id', c2.concept_name AS 'c2.concept_name', 
  c2.concept_code AS 'c2.concept_code', c2.concept_class_id AS 'c2.concept_class_id',
  c2.vocabulary_id AS 'c2.vocabulary_id' 
  FROM concept AS c1 
  LEFT JOIN concept_relationship AS r 
  ON r.concept_id_1 = c1.concept_id
  LEFT JOIN concept AS c2
  ON c2.concept_id = r.concept_id_2
  WHERE c2.vocabulary_id = 'ATC'
  AND (c2.concept_code LIKE 'A10A%' OR c2.concept_code LIKE 'A10B%')";
  
  



all_meds = dbGetQuery(mydb, query);

drug_concept_id_list <- unique(all_meds$concept_id);




concept_id_to_code <- list();

for(iii in 1:nrow(all_meds)){
  
  concept_id_to_code[[as.character(all_meds$concept_id[iii])]] <- all_meds$c2.concept_code[iii]
  
}


concept_id_to_name <- list();
concept_id_to_class <- list();


for(iii in 1:nrow(all_meds)){
  concept_id_to_name[[as.character(all_meds$concept_id[iii])]] <- all_meds$concept_name[iii]
  
  this_class <- "OTHER"
  this_atc <- all_meds$c2.concept_code[iii]
  
  if(grepl("A10A", this_atc)){
    this_class <- "INSULIN"
  }
  
  if(grepl("A10BA02", this_atc)){
    this_class <- "METFORMIN"
  }
  
  if(grepl("A10BB", this_atc)){
    this_class <- "Sulfonylureas"
  }
  
  if(grepl("A10BF", this_atc)){
    this_class <- "Alpha_Glucosidase_Inhibitor"
  }
  
  if(grepl("A10BG", this_atc)){
    this_class <- "TZD"
  }
  
  if(grepl("A10BH", this_atc)){
    this_class <- "DPP-4 Inhibitor"
  }
  
  if(grepl("A10BJ|A10BX04|A10BX13|A10BX14|A10BX10|A10BX07", this_atc)){
    this_class <- "GLP1_RA"
  }
  
  
  if(grepl("A10BK|A10BX12|A10BX11|A10BX09", this_atc)){
    this_class <- "SGLT2_Inhibitor"
  }
  
  
  if(grepl("A10BX02|A10BX03|A10BX08|A10BX05", this_atc)){
    this_class <- "Meglitinides"
  }
  
  
  if(grepl("A10BA01", this_atc)){
    this_class <- "OTHER" #Phenformin
  }
  
  if(grepl("A10BA03", this_atc)){
    this_class <- "OTHER" #Buformin
  }
  
  
  if(grepl("A10BX01|A10BX05|A10BX06", this_atc)){
    this_class <- "OTHER"
  }
  
  ##############
  ##############
  ##############
  ##############
  #combinations
  ##############
  ##############
  ##############
  #A10BD01 Phenformin and sulfonylureas
  if(grepl("A10BD01", this_atc)){
    this_class <- "Sulfonylureas|METFORMIN"
  }
  
  #A10BD02 Metformin and sulfonylureas
  if(grepl("A10BD02", this_atc)){
    this_class <- "Sulfonylureas|METFORMIN"
  }
  
  #A10BD03 Metformin and rosiglitazone
  if(grepl("A10BD03", this_atc)){
    this_class <- "TZD|METFORMIN"
  }
  
  #A10BD04 Glimepiride and rosiglitazone
  if(grepl("A10BD04", this_atc)){
    this_class <- "TZD|Sulfonylureas"
  }
  
  #A10BD05 Metformin and pioglitazone
  if(grepl("A10BD05", this_atc)){
    this_class <- "TZD|METFORMIN"
  }
  
  #A10BD06 Glimepiride and pioglitazone
  if(grepl("A10BD06", this_atc)){
    this_class <- "TZD|Sulfonylureas"
  }
  
  #A10BD07 Metformin and sitagliptin
  if(grepl("A10BD07", this_atc)){
    this_class <- "METFORMIN|DPP-4 Inhibitor"
  }
  
  #A10BD08 Metformin and vildagliptin
  if(grepl("A10BD08", this_atc)){
    this_class <- "METFORMIN|DPP-4 Inhibitor"
  }
  
  #A10BD09 Pioglitazone and alogliptin
  if(grepl("A10BD09", this_atc)){
    this_class <- "METFORMIN|DPP-4 Inhibitor"
  }
  
  #A10BD10 Metformin and saxagliptin
  if(grepl("A10BD10", this_atc)){
    this_class <- "METFORMIN|DPP-4 Inhibitor"
  }
  
  #A10BD11 Metformin and linagliptin
  if(grepl("A10BD11", this_atc)){
    this_class <- "METFORMIN|DPP-4 Inhibitor"
  }
  
  #A10BD12 Pioglitazone and sitagliptin
  if(grepl("A10BD12", this_atc)){
    this_class <- "TZD|DPP-4 Inhibitor"
  }
  
  #A10BD13 Metformin and alogliptin
  if(grepl("A10BD13", this_atc)){
    this_class <- "METFORMIN|DPP-4 Inhibitor"
  }
  
  #A10BD14 Metformin and repaglinide
  if(grepl("A10BD14", this_atc)){
    this_class <- "METFORMIN|Meglitinides"
  }
  
  #A10BD15 Metformin and dapagliflozin
  if(grepl("A10BD15", this_atc)){
    this_class <- "SGLT2_Inhibitor|METFORMIN"
  }
  
  #A10BD16 Metformin and canagliflozin
  if(grepl("A10BD16", this_atc)){
    this_class <- "SGLT2_Inhibitor|METFORMIN"
  }
  
  #A10BD17 Metformin and acarbose
  if(grepl("A10BD17", this_atc)){
    this_class <- "METFORMIN|Alpha_Glucosidase_Inhibitor"
  }
  
  #A10BD18 Metformin and gemigliptin
  if(grepl("A10BD18", this_atc)){
    this_class <- "METFORMIN|DPP-4 Inhibitor"
  }
  
  #A10BD19 Linagliptin and empagliflozin
  if(grepl("A10BD19", this_atc)){
    this_class <- "SGLT2_Inhibitor|DPP-4 Inhibitor"
  }
  
  #A10BD20 Metformin and empagliflozin
  if(grepl("A10BD20", this_atc)){
    this_class <- "SGLT2_Inhibitor|METFORMIN"
  }
  
  #A10BD21 Saxagliptin and dapagliflozin
  if(grepl("A10BD21", this_atc)){
    this_class <- "SGLT2_Inhibitor|DPP-4 Inhibitor"
  }
  
  #A10BD22 Metformin and evogliptin
  if(grepl("A10BD22", this_atc)){
    this_class <- "METFORMIN|DPP-4 Inhibitor"
  }
  
  #A10BD23 Metformin and ertugliflozin
  if(grepl("A10BD23", this_atc)){
    this_class <- "SGLT2_Inhibitor|METFORMIN"
  }
  
  #A10BD24 Sitagliptin and ertugliflozin
  if(grepl("A10BD24", this_atc)){
    this_class <- "SGLT2_Inhibitor|DPP-4 Inhibitor"
  }
  
  #A10BD25 Metformin, saxagliptin and dapagliflozin
  if(grepl("A10BD25", this_atc)){
    this_class <- "SGLT2_Inhibitor|METFORMIN|DPP-4 Inhibitor"
  }
  
  
  
  concept_id_to_class[[as.character(all_meds$concept_id[iii])]] <- this_class
  
}













#this commented out query is for a fix if ATC codes don't match up correctly, which is currently the case

if(TRUE){
  query <- "
  SELECT * FROM concept
    WHERE 
    concept_name LIKE '%canagliflozin%' OR
    concept_name LIKE '%dapagliflozin%' OR
    concept_name LIKE '%empagliflozin%' OR
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
  ";

  
  
  
  all_meds = dbGetQuery(mydb, query);
  
  
  
  
  for(iii in 1:nrow(all_meds)){
    concept_id_to_name[[as.character(all_meds$concept_id[iii])]] <- all_meds$concept_name[iii]
    
    this_class <- "OTHER"
    this_atc <- all_meds$c2.concept_code[iii]
    
    
    ###############
    ###############
    ############### This section manually sets classes using names because the ATC mapping is currently incomplete.
    ###############
    ###############
    
    
    this_name <- all_meds$concept_name[iii];
    
    if(grepl("metformin", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "METFORMIN"
      }else if(!grepl("METFORMIN", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "METFORMIN", sep="|")
        
      }
    }
    
    if(grepl("canagliflozin", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "SGLT2_Inhibitor"
      }else if(!grepl("SGLT2_Inhibitor", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "SGLT2_Inhibitor", sep="|")
        
      }
    }
    
    
    if(grepl("dapagliflozin", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "SGLT2_Inhibitor"
      }else if(!grepl("SGLT2_Inhibitor", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "SGLT2_Inhibitor", sep="|")
        
      }
    }
    
    if(grepl("empagliflozin", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "SGLT2_Inhibitor"
      }else if(!grepl("SGLT2_Inhibitor", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "SGLT2_Inhibitor", sep="|")
        
      }
    }
    
    
    
    if(grepl("liraglutide", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "GLP1_RA"
      }else if(!grepl("GLP1_RA", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "GLP1_RA", sep="|")
        
      }
    }
    
    if(grepl("albiglutide", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "GLP1_RA"
      }else if(!grepl("GLP1_RA", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "GLP1_RA", sep="|")
        
      }
    }
    
    if(grepl("dulaglutide", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "GLP1_RA"
      }else if(!grepl("GLP1_RA", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "GLP1_RA", sep="|")
        
      }
    }
    
    if(grepl("semaglutide", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "GLP1_RA"
      }else if(!grepl("GLP1_RA", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "GLP1_RA", sep="|")
        
      }
    }
    
    
    
    if(grepl("exenatide", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "GLP1_RA"
      }else if(!grepl("GLP1_RA", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "GLP1_RA", sep="|")
        
      }
    }
    
    if(grepl("lixisenatide", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "GLP1_RA"
      }else if(!grepl("GLP1_RA", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "GLP1_RA", sep="|")
        
      }
    }
    
    if(grepl("repaglinide", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "Meglitinides"
      }else if(!grepl("Meglitinides", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "Meglitinides", sep="|")
        
      }
    }
    
    if(grepl("nateglinide", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "Meglitinides"
      }else if(!grepl("Meglitinides", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "Meglitinides", sep="|")
        
      }
    }
    
    
    
    if(grepl("acarbose", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "Alpha_Glucosidase_Inhibitor"
      }else if(!grepl("Alpha_Glucosidase_Inhibitor", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "Alpha_Glucosidase_Inhibitor", sep="|")
        
      }
    }
    
    if(grepl("miglitol", this_name, ignore.case=TRUE)){
      if(this_class == "OTHER"){
        this_class <- "Alpha_Glucosidase_Inhibitor"
      }else if(!grepl("Alpha_Glucosidase_Inhibitor", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "Alpha_Glucosidase_Inhibitor", sep="|")
        
      }
    }
    
    if(grepl("glyburide", this_name, ignore.case=TRUE) ||
       grepl("glimepiride", this_name, ignore.case=TRUE) ||
       grepl("glipizide", this_name, ignore.case=TRUE) ||
       grepl("chlorpropamide", this_name, ignore.case=TRUE) ||
       grepl("tolazamide", this_name, ignore.case=TRUE) ||
       grepl("tolbutamide", this_name, ignore.case=TRUE)
    ){
      if(this_class == "OTHER"){
        this_class <- "Sulfonylureas"
      }else if(!grepl("Sulfonylureas", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "Sulfonylureas", sep="|")
        
      }
    }
    
    
    if(grepl("sitagliptin", this_name, ignore.case=TRUE) ||
       grepl("linagliptin", this_name, ignore.case=TRUE) ||
       grepl("saxagliptin", this_name, ignore.case=TRUE) ||
       grepl("alogliptin", this_name, ignore.case=TRUE) 
    ){
      if(this_class == "OTHER"){
        this_class <- "DPP-4 Inhibitor"
      }else if(!grepl("DPP-4 Inhibitor", this_class, ignore.case=TRUE)){
        #if it didn't already pick up the correct class with ATC codes
        
        this_class <- paste(this_class, "DPP-4 Inhibitor", sep="|")
        
      }
    }
    
    
    
    #remove possible duplicate med classes, order by reverse alphabetical order
    
    #split by pipes
    med_list <- strsplit(this_class, '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
    
    
    #remove duplicates; sort
    med_list <- med_list[order(med_list, decreasing=TRUE)]
    med_list <- unique(med_list)
    
    #put string back together
    this_class <- paste(med_list, collapse="|")
    
    
    ###############
    ###############
    ############### End section for fixing ATCs
    ###############
    ###############
    
    
    concept_id_to_class[[as.character(all_meds$concept_id[iii])]] <- this_class
    
  }
  
}#end ATC mapping fix






















#####################
#####################
##################### Loop through patients to find index dates
#####################
#####################

if(debugit){ print("Looping through patients to find index dates"); }

all_med_dates$person_id <- as.numeric(all_med_dates$person_id)
all_med_dates$provider_id <- as.numeric(all_med_dates$provider_id)
all_med_dates$visit_occurrence_id <- as.numeric(all_med_dates$visit_occurrence_id)

patids <- unique(all_med_dates$person_id)

#setup progress bar
cl <- makeSOCKcluster(num_cores)
registerDoSNOW(cl)
iterations <- length(patids)
pb <- txtProgressBar(max = iterations, style = 3)
progress <- function(n) setTxtProgressBar(pb, n)
opts <- list(progress = progress)






medication_dates_table <- data.frame();

medication_dates_table <- foreach(iii=1:length(patids), .combine=rbind, 
                                  .options.snow=opts) %dopar% {
            
#for(iii in 1:length(patids)){
#print(iii)

    
    
    this_patid = patids[iii]
    
    
    
    
    med_result <- all_med_dates[which(all_med_dates$person_id == this_patid),];
    
    #some patients have actual sites but are listed as NA for some meds.
    #here, we set NA site to the other ones in the history if there are multiple
    if( length(which(is.na(med_result$site))) > 0){
      if( length(which(!is.na(med_result$site))) > 0 ){
        med_result$site[which(is.na(med_result$site))] <- med_result$site[which(!is.na(med_result$site))[1]]
      }
    }
    
    
    med_result$Most_Recent_Drug <- NA
    
    med_result$Most_Recent_Drug <- max(med_result$drug_exposure_start_date)
    
    
    
    med_result$Current_Medications_Noclass <- "NONE"
    med_result$Current_Medications <- "NONE"

    
    if(nrow(med_result) > 0){
      
      
      for(mmm in 1:nrow(med_result)){
        #print(mmm)
        
        #grab information from stored tables
        
        tryCatch({ med_result$Current_Medications[mmm] <- concept_id_to_class[[as.character(med_result$drug_concept_id[mmm])]]}, error= function(e){} )
        tryCatch({ med_result$Current_Medications_Noclass[mmm] <- concept_id_to_name[[as.character(med_result$drug_concept_id[mmm])]]}, error= function(e){} )
        
        
      }
      
    }
    
    
      #clean just in case---this should not be needed.
    med_result <- med_result[which(med_result$Current_Medications != "NONE"),]
    


    if(nrow(med_result) > 0){
      #find unique dates
      these_medications <- med_result[which(med_result$person_id == this_patid),]
      these_medications <- these_medications[order(as.Date(these_medications$drug_exposure_start_date)),]
      
      
        #here we find out which medications are being prescribed by one of our providers
        #and which are self-reported or in the medication list
      ind1 <- which(these_medications$drug_type_concept_name != "Patient Self-Reported Medication")
      ind2 <- which(these_medications$drug_type_concept_name != "Medication list entry")
      ind_non_historical <- unique(c(ind1, ind2))
      
      ind1 <- which(these_medications$drug_type_concept_name == "Patient Self-Reported Medication")
      ind2 <- which(these_medications$drug_type_concept_name == "Medication list entry")
      ind_historical <- unique(c(ind1, ind2))
      
      meds_non_historical <- these_medications[ind_non_historical,];
      meds_historical <- these_medications[ind_historical,];
      
      tmp_dates <- meds_non_historical$drug_exposure_start_date
      tmp_dates <- tmp_dates[!is.na(tmp_dates)]
      tmp_dates <- unique(tmp_dates)
      tmp_dates <- tmp_dates[order(as.Date(tmp_dates))]
      
      
      
      
      these_dates <- c();
      
      #remove dates that are too close together using sliding_window_for_dates
      if(length(tmp_dates) > 0){
        for(iii in length(tmp_dates):1){
          add_date <- 1;
          if(length(these_dates) > 0){
            
            for(fff in 1:length(these_dates)){
              if( abs( as.Date(these_dates[fff]) - as.Date(tmp_dates[iii]) ) <= sliding_window_for_dates ){
                add_date <- 0;
              }
            }
          }
          
          if(add_date == 1){
            if(length(these_dates) == 0){
              these_dates <- c(as.Date(tmp_dates[iii]))
            }else{
              these_dates <- c(these_dates, as.Date(tmp_dates[iii]))
            }
          }
        }
      }
      
      
      
      
      
      #reorder
      if(length(these_dates) > 0){
        
        these_dates <- these_dates[order(as.Date(these_dates))]
      }
      
      
    
        #find which date is an index date,
        #set all meds according to their dates
      if(length(these_dates) > 0){
        #loop through all start and stop dates (unique dates)
        
        
        #construct index date table for this patient
        these_indexes <- data.frame(these_dates);
        colnames(these_indexes)[1] <- "index_date"
        
        these_indexes[,1] <- as.character(these_indexes[,1])
        
        these_indexes$person_id <- this_patid
        these_indexes$site <- med_result[1,]$site
        these_indexes$provider_id <- med_result[1,]$provider_id
        these_indexes$visit_occurrence_id <- med_result[1,]$visit_occurrence_id
        these_indexes$Current_Medications <- "NONE"
        these_indexes$Current_Medications_Noclass <- "NONE"
        these_indexes$Previous_Medication <- "NONE"
        these_indexes$Previous_Medication_Noclass <- "NONE"
        these_indexes$Med_Increase <- "NONE"
        these_indexes$Med_Increase_Noclass <- "NONE"
        these_indexes$Time_To_Index_Date_Change <- -1
        these_indexes$dosage_change_only <- 0
        
        for(jjj in 1:nrow(these_indexes)){
          unique_start_or_stop_date <- these_indexes[jjj,]$index_date
          
          
          #loop through all start and stop dates (non-unique dates) to find more than one medication being taken at once
          for(hhh in 1:nrow(these_medications)){
            #print(hhh)
            if( ( 
              is.na(these_medications$drug_exposure_start_date[hhh]) || 
              as.Date(unique_start_or_stop_date) >= (as.Date(these_medications$drug_exposure_start_date[hhh]) - sliding_window_for_dates) 
            )  &&
            (
              is.na(these_medications$drug_exposure_end_date[hhh]) || 
              (
                !is.na(these_medications$drug_exposure_end_date[hhh]) && 
                (as.Date(these_medications$drug_exposure_end_date[hhh]) + sliding_window_for_dates) > as.Date(unique_start_or_stop_date)
              ) 
            ) 
            ) {
              
              #this medication started on or before this index date and ended after this index date
              if(these_indexes$Current_Medications[jjj] == "NONE"){
                #nothing saved yet for jjj
                these_indexes$Current_Medications_Noclass[jjj] <- these_medications$Current_Medications_Noclass[hhh]
                these_indexes$Current_Medications[jjj] <- these_medications$Current_Medications[hhh]
                these_indexes$concept_name[jjj] <- these_medications$concept_name[hhh]
                
              }else{ #something is already saved in jjj
                
                
                #add new medication (class)
                curr_meds <- these_indexes$Current_Medications[jjj]
                new_meds <- paste(curr_meds, these_medications[hhh,]$Current_Medications, sep="|")
                
                #split by pipes
                med_list <- strsplit(new_meds, '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
                
                
                #remove duplicates; sort
                med_list <- med_list[order(med_list, decreasing=TRUE)]
                med_list <- unique(med_list)
                
                #put string back together
                these_indexes$Current_Medications[jjj] <- paste(med_list, collapse="|")
                
                
                #add new medication (noclass)
                curr_meds <- these_indexes$Current_Medications_Noclass[jjj]
                new_meds <- paste(curr_meds, these_medications[hhh,]$Current_Medications_Noclass, sep="|")
                
                #split by pipes
                med_list <- strsplit(new_meds, '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
                
                
                #remove duplicates; sort
                med_list <- med_list[order(med_list, decreasing=TRUE)]
                med_list <- unique(med_list)
                
                #put string back together
                
                these_indexes$Current_Medications_Noclass[jjj] <- paste(med_list, collapse="|")
                

              }
              
            }
            
          }
          
        }
        
      }
      
      
      
      if(nrow(these_indexes) > 1){
        
        #####Class
        
        #find previous
        for(jjj in 2:nrow(these_indexes)){
          these_indexes$Previous_Medication[jjj] <- these_indexes$Current_Medications[(jjj - 1)]
        }
        
        
        
        
        
        #####Noclass
        
        #find previous
        for(jjj in 2:nrow(these_indexes)){
          these_indexes$Previous_Medication_Noclass[jjj] <- these_indexes$Current_Medications_Noclass[(jjj - 1)]
        }
        
        
        
        
        
      }
      
      
      
        #set historical medications as previous medications
      if(length(these_dates) > 0 && nrow(meds_historical) > 0){
        #loop through all start and stop dates (unique dates)
        
        for(jjj in 1:nrow(these_indexes)){
          unique_start_or_stop_date <- these_indexes[jjj,]$index_date
          
          
          #loop through all start and stop dates (non-unique dates) to find more than one medication being taken at once
          for(hhh in 1:nrow(meds_historical)){
            #print(hhh)
            if( ( 
              is.na(meds_historical$drug_exposure_start_date[hhh]) || 
              as.Date(unique_start_or_stop_date) >= (as.Date(meds_historical$drug_exposure_start_date[hhh]) - sliding_window_for_dates) 
            )  &&
            (
              is.na(meds_historical$drug_exposure_end_date[hhh]) || 
              (
                !is.na(meds_historical$drug_exposure_end_date[hhh]) && 
                (as.Date(meds_historical$drug_exposure_end_date[hhh]) + sliding_window_for_dates) > as.Date(unique_start_or_stop_date)
              ) 
            ) 
            ) {
              
              #this medication started on or before this index date and ended after this index date
              if(these_indexes$Previous_Medication[jjj] == "NONE"){
                #nothing saved yet for jjj
                these_indexes$Previous_Medication_Noclass[jjj] <- meds_historical$Current_Medications_Noclass[hhh]
                these_indexes$Previous_Medication[jjj] <- meds_historical$Current_Medications[hhh]

              }else{ #something is already saved in jjj
                
                
                #add new medication (class)
                curr_meds <- these_indexes$Previous_Medication[jjj]
                new_meds <- paste(curr_meds, meds_historical[hhh,]$Current_Medications, sep="|")
                
                #split by pipes
                med_list <- strsplit(new_meds, '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
                
                
                #remove duplicates; sort
                med_list <- med_list[order(med_list, decreasing=TRUE)]
                med_list <- unique(med_list)
                
                #put string back together
                these_indexes$Previous_Medication[jjj] <- paste(med_list, collapse="|")
                
                
                #add new medication (noclass)
                curr_meds <- these_indexes$Previous_Medication_Noclass[jjj]
                new_meds <- paste(curr_meds, meds_historical[hhh,]$Current_Medications_Noclass, sep="|")
                
                #split by pipes
                med_list <- strsplit(new_meds, '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
                
                
                #remove duplicates; sort
                med_list <- med_list[order(med_list, decreasing=TRUE)]
                med_list <- unique(med_list)
                
                #put string back together
                
                these_indexes$Previous_Medication_Noclass[jjj] <- paste(med_list, collapse="|")
                

              }
              
            }
            
          }
          
        }
        
      }
      
      
      
        #for each index date, find the previous and next medication for this patient
      
      if(nrow(these_indexes) == 1){
        
      
        #split by pipes
        prev <- strsplit(these_indexes$Previous_Medication[1], '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
        curr <- strsplit(these_indexes$Current_Medications[1], '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
        
        
        #put string back together
        these_indexes$Med_Increase[1] <- paste(curr[!(curr %in% prev)], collapse="|")
        
        
        
        #split by pipes
        prev <- strsplit(these_indexes$Previous_Medication_Noclass[1], '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
        curr <- strsplit(these_indexes$Current_Medications_Noclass[1], '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
        
        
        #put string back together
        these_indexes$Med_Increase_Noclass[1] <- paste(curr[!(curr %in% prev)], collapse="|")
        
        
        
      }else if(nrow(these_indexes) > 1){
        
        
        #find med increase
        for(jjj in 1:nrow(these_indexes)){
          
          #split by pipes
          prev <- strsplit(these_indexes$Previous_Medication[jjj], '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
          curr <- strsplit(these_indexes$Current_Medications[jjj], '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
          
          
          #put string back together
          these_indexes$Med_Increase[jjj] <- paste(curr[!(curr %in% prev)], collapse="|")
          
          
        }
        
        
        #find med increase noclass
        for(jjj in 1:nrow(these_indexes)){
          
          #split by pipes
          prev <- strsplit(these_indexes$Previous_Medication_Noclass[jjj], '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
          curr <- strsplit(these_indexes$Current_Medications_Noclass[jjj], '\\|', fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
          
          
          #put string back together
          these_indexes$Med_Increase_Noclass[jjj] <- paste(curr[!(curr %in% prev)], collapse="|")
          
          
        }
        
    }
    
  }
  
  
  
  # make Time_To_Index_Date_Change
    
  if(nrow(these_indexes)!= 0){
    
    these_indexes$Time_To_Index_Date_Change <- -1;
    
    for(ddd in 1:nrow(these_indexes)){
      
      
      if(ddd == nrow(these_indexes)){
        #these_indexes$Time_To_Index_Date_Change[ddd] = max(med_result$Most_Recent_Drug)
      }else{
        
        these_indexes$Time_To_Index_Date_Change[ddd] = as.Date(these_indexes$index_date[(ddd + 1)]) - as.Date(these_indexes$index_date[ddd])
        
      }
      
      
    }
  }
  
  #make indicator if this is a change in doage, not a change in class.
  if(nrow(these_indexes)!= 0){
    these_indexes$dosage_change_only <- 0;
    
    if(nrow(these_indexes) > 1){
      for(ddd in 2:nrow(these_indexes)){
        if(these_indexes$Previous_Medication[ddd] == these_indexes$Current_Medications[ddd]){
          these_indexes$dosage_change_only[ddd] <- 1;
        }
      }
    }
  }
  
    #grab only medication changes
  these_indexes <- these_indexes[which(these_indexes$Previous_Medication_Noclass != these_indexes$Current_Medications_Noclass),];
  
  #print("dim(these_indexes)")
  #print(dim(these_indexes))
  #print(these_indexes)
  
  #medication_dates_table <- rbind(medication_dates_table,  these_indexes)
  if(nrow(these_indexes)!= 0){ return(these_indexes) }
  
}


stopCluster(cl)


medication_dates_table[which(is.na(medication_dates_table$site)),]$site <- "OTHER"



#reorder them
medication_dates_table <- medication_dates_table[order(medication_dates_table$person_id, as.Date(medication_dates_table$index_date)),]



if(debugit){
  print("saving Rdata...");
  save.image();
  print("done saving Rdata");
}



print("After collapsing index dates:")
print("dim(medication_dates_table)")
print(dim(medication_dates_table))


print("length(unique(medication_dates_table$person_id))")
print(length(unique(medication_dates_table$person_id)))


first_pat_occ <- medication_dates_table[match(unique(medication_dates_table$person_id), medication_dates_table$person_id),]; #find first instance of each pat

print("For first patient occurrence:")

print("length(unique(first_pat_occ$person_id))")
print(length(unique(first_pat_occ$person_id)))


print("table(first_pat_occ$site)")
print(table(first_pat_occ$site))





if(debugit){ 
  print("saving Rdata...");
  save.image();
  print("done saving Rdata");
}















#here make sure if patient goes off medications we stop tracking them, even if they start again later

prev_pat_id <- medication_dates_table$person_id[1];
found_med_stop = 0;

for(ddd in 1:nrow(medication_dates_table)){
  
  if(ddd == 1 || ddd %% 1000 == 0){ print(ddd) }
  
  if(prev_pat_id != medication_dates_table$person_id[ddd]){
    #new patient id
    prev_pat_id = medication_dates_table$person_id[(ddd - 1)];
    
    found_med_stop = 0;
    
  }
  
  
  
  if(medication_dates_table$Current_Medications[ddd] == "NONE"){
    found_med_stop = 1;
  }
  
  if(found_med_stop == 1){
    medication_dates_table$Current_Medications[ddd] = "NONE";
  }
  
  
}
















full_final_upload <- medication_dates_table
full_final_upload$Grouping <- "ALL"



#################
#################
################# Save analysis cohorts
#################
#################



#full_final_upload$dosage_change_only <- NULL;

#full_final_upload <- full_final_upload[grep("Insulin", full_final_upload$Current_Medications, ignore.case=TRUE, invert=TRUE),];



if(debugit){ 
  print("saving Rdata...");
  save.image();
  print("done saving Rdata");
}




print("dim(full_final_upload)")
print(dim(full_final_upload))
print("dim(full_final_upload[which(full_final_upload$dosage_change_only != 1),])")
print(dim(full_final_upload[which(full_final_upload$dosage_change_only != 1),]))
print("table(full_final_upload$Grouping)")
print(table(full_final_upload$Grouping))
print("table(full_final_upload[which(full_final_upload$dosage_change_only != 1),]$Grouping)")
print(table(full_final_upload[which(full_final_upload$dosage_change_only != 1),]$Grouping))


####################
#################### Upload table
####################

if(debugit){ print("Guide: Uploading index dates..."); }



query <- paste0("DROP TABLE IF EXISTS ",local_db_name,".dbo.medication_index_dates;");

aaa = dbGetQuery(mydb, query);

   
query <- paste0("CREATE TABLE ",local_db_name,".dbo.medication_index_dates ( 
                index_date Date,
                person_id bigint,
                site Varchar (255),
                provider_id bigint,
                visit_occurrence_id bigint,
                Current_Medications Varchar (255),
                Current_Medications_Noclass Varchar (765),
                Previous_Medication Varchar (255),
                Previous_Medication_Noclass Varchar (765),
                Med_Increase Varchar (255),
                Med_Increase_Noclass  Varchar (765),
                Time_To_Index_Date_Change Varchar (255),
                dosage_change_only Bit,
                Grouping Varchar (255)
                
);");



aaa = dbGetQuery(mydb, query);




print(paste0("Uploading for ", nrow(full_final_upload), " index dates"))

for(iii in 1:nrow(full_final_upload)){
  #for(iii in 1:50){
  if(iii == 1 || iii %% 10000 == 0){ print(iii) }
  
  provider <- full_final_upload[iii,]$provider_id
  
  if(is.na(provider)){ provider <- "NULL" }
  
  query <- paste0("INSERT INTO ",local_db_name,".dbo.medication_index_dates VALUES ('", 
                  full_final_upload[iii,]$index_date, "', ",
                  full_final_upload[iii,]$person_id, ", '",
                  full_final_upload[iii,]$site, "', ",
                  provider, ", ",
                  full_final_upload[iii,]$visit_occurrence_id, ", '",
                  paste(as.character(full_final_upload[iii,6:ncol(full_final_upload)]), collapse="', '" ) ,"')")
  
  aaa = dbGetQuery(mydb, query) ;
  
}












print("upload complete")


