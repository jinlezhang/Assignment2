library(RODBC)
conn<-odbcConnect("TDP3")
today<-paste0(substr(as.character(Sys.Date()),9,10),"/", substr(as.character(Sys.Date()),6,7),"/", substr(as.character(Sys.Date()),1,4))
# today="30/11/2016"

ADJ_FACT<-sqlQuery(conn, paste0("select ENTERED_DATE, count(*) No_of_New_Rows, sum(EAD_AMT) as Total_EAD from WDP1SYN.DW_GRP_CR_ADJ_FACT where ENTERED_DATE=cast('", today, "' as Date format 'DD/MM/YYYY') group by ENTERED_DATE"))

# FCLY_AR_FACT<-sqlQuery(conn, paste0("select count(*) No_of_New_Rows, sum(EAD_AMT) as Total_EAD from WDP1SYN.DW_GRP_CR_FCLY_AR_FACT where ENTERED_DATE=cast('", today, "' as Date format 'DD/MM/YYYY')"))
# 
# FCLY_RPRT_FACT<-sqlQuery(conn, paste0("select count(*) No_of_New_Rows, sum(EAD_AMT) as Total_EAD from WDP1SYN.DW_GRP_CR_FCLY_RPRT_FACT where ENTERED_DATE=cast('", today, "' as Date format 'DD/MM/YYYY')"))
# 
# FCLY_STAT_FACT<-sqlQuery(conn, paste0("select count(*) No_of_New_Rows, sum(EAD_AMT) as Total_EAD from WDP1SYN.DW_GRP_CR_FCLY_STAT_FACT where ENTERED_DATE=cast('", today, "' as Date format 'DD/MM/YYYY')"))
# 
# FINAL_FACT<-sqlQuery(conn, paste0("select count(*) No_of_New_Rows, sum(EAD_AMT) as Total_EAD from WDP1SYN.DW_GRP_CR_FINAL_FACT where ENTERED_DATE=cast('", today, "' as Date format 'DD/MM/YYYY')"))
# 
# IP_FACT<-sqlQuery(conn, paste0("select count(*) No_of_New_Rows, sum(EAD_AMT) as Total_EAD from WDP1SYN.DW_GRP_CR_IP_FACT where ENTERED_DATE=cast('", today, "' as Date format 'DD/MM/YYYY')"))
# 
# PD_FACT<-sqlQuery(conn, paste0("select count(*) No_of_New_Rows, sum(EAD_AMT) as Total_EAD from WDP1SYN.DW_GRP_CR_PD_FACT where ENTERED_DATE=cast('", today, "' as Date format 'DD/MM/YYYY')"))
# 
# PD_LGD_FACT<-sqlQuery(conn, paste0("select count(*) No_of_New_Rows, sum(EAD_AMT) as Total_EAD from WDP1SYN.DW_GRP_CR_PD_LGD_FACT where ENTERED_DATE=cast('", today, "' as Date format 'DD/MM/YYYY')"))
# 
# RPRT_FACT<-sqlQuery(conn, paste0("select count(*) No_of_New_Rows, sum(EAD_AMT) as Total_EAD from WDP1SYN.DW_GRP_CR_RPRT_FACT where ENTERED_DATE=cast('", today, "' as Date format 'DD/MM/YYYY')"))

ADJ_FACT_old<-readRDS("C:/Users/M043377.old/Documents/ADJ_FACT_RDS.rds")

ADJ_FACT<-rbind(ADJ_FACT_old,ADJ_FACT)

saveRDS(ADJ_FACT, "C:/Users/M043377.old/Documents/ADJ_FACT_RDS.rds")


closeAllConnections()