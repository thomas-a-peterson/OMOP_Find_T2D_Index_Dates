#!/bin/bash


"C:\Program Files\R\R-3.5.0\bin\Rscript" T2D_Find_Patients.R OMOP_DeID peterst1 info-dwsql04dev UCWay_DrugPathways > .\log\nohup_out_T2D_Find_Patients.txt 

"C:\Program Files\R\R-3.5.0\bin\Rscript" T2D_Join_For_Medication_Dates.R OMOP_DeID peterst1 info-dwsql04dev UCWay_DrugPathways > .\log\nohup_out_T2D_Join_For_Medication_Dates.txt 

"C:\Program Files\R\R-3.5.0\bin\Rscript" T2D_Make_Index_Dates.R OMOP_DeID peterst1 info-dwsql04dev UCWay_DrugPathways > .\log\nohup_out_T2D_make_index_dates.txt 

"C:\Program Files\R\R-3.5.0\bin\Rscript" T2D_Join_For_Factors.R OMOP_DeID peterst1 info-dwsql04dev UCWay_DrugPathways > .\log\nohup_out_T2D_Join_For_Factors.txt 





