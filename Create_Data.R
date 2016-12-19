
library(aws.s3)
library(ggplot2)
library(assertthat)
library(dplyr)
library(sqldf)
library(parallel)
library(R6)
library(snow)
library(mvtnorm)
library(itertools)
library(doParallel)
library(iterators)
library(MASS)
library(data.handling)
library(RODBC)
library(lubridate)
library(zoo)
library(Rblpapi) #***add call to this library**
library(Rsolnp)
library(tm) #'text miner' library for reading pdf 
library(nloptr)
library(plotly)
library(shiny)
library(tidyr)
library(roxygen2)
library(data.handling)
library(stringr)


##add PATH to bin folder of github in environment variable first (usually C:/Program Files/Git/bin)

##add the following to .gitconfig file which can be found at  'C:\Users\[username]'
#[credential]
#	helper = wincred
 
##generate security keys by following the below steps inside Git Shell

#change directory to local repository
#$cd [path/to/repos]                 

#generate key
#$ ssh-keygen -t rsa -b 4096 -C "[email address]"

#copy the key created  (this will paste the values when doing ctrl-v
#$ clip < ~/.ssh/id_rsa.pub

#go to github account in the settings page where you can add keys (https://github.com/settings/ssh)

#1. SET WD TO LOCAL GITHUB REPOSITORY, AND SYNC TO LATEST GITHUB VERSION--------------------------------
 
##synchronise local github repository 
#set environment variable's HOME path
Sys.setenv("HOME"=paste0("C:/Users/",Sys.info()[["user"]]))


setwd(paste0("C:/Users/",Sys.info()[["user"]],"/Documents"))
mainWD<-getwd()

#set working directory to where github local repos is => this runs off PW machine
setwd(paste0("C:/Users/",Sys.info()[["user"]],"/Documents/GitHub/OilModel"))

# Load path name and data loader code
source("server/path_name.R")
source("server/data_loader.R")


#synchronise local github repository 
#system("git pull") # doesn't work - can't sync - just working off last version on C:/

#sourcing R scripts
source("OverNight/FPC & Spot4.R") 


start.date<-as.Date("2000/1/1")
Today<-Sys.Date()
Ndir<-format(Today,"%Y%m%d")

FPC.start.date<-seq(as.Date(format(Today, "%Y/%m/01")), by="month", length=2)[2]### always update to the latest month" change name to start !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
FPC.end.date<-seq(from=as.Date(FPC.start.date), by="month", length.out = 36)[36]

#2. BLOOMBERG TO DB UPLOAD-------------------------------------------------------------------------------

#change working directory
setwd("//ENESYDND01/energy/enct/Strategic Risk/Critical Models/5_Oil/Oil data/2015/Roil")

dt<-data.handling::list.data(TRUE, "Shiny App4/2. Overnight Scripts/Oil tickers.csv",
                             start.date, end.date = Today) # no hard codes!!!****
data.handling::save_prices_to_access(dt) #calls PDF uploader to upload JCC and JKM from GDF emailed PDF into DB


option_Bloomberg<-option_price_downloader(Today, 36)
# change the column name from Month to Month_Number in order to join brent option table into master lookup table (column name "Month" appears twice in the master lookup table)
option_price_writer(option_Bloomberg)


# save PDF JKM and JCC FPC into Access
# options(error = NULL)
savePDF()
# save excel production data into R and Access
#data.handling::save_production_to_RAccess()

# Noting here that volumes are already in DB and dont need to be uploaded

#3. READ DB PRICE AND VOLUME DATA INTO GLOBAL ENVIRONMENT---------------------------------------------------

Var_start<-c(.3,.1,.3,.3, .1,.1,.1,.1,.1,.1,.1,.1,.1)^2/12 #need to have implied vol (front month ATM) table in DB*******


#   3.1. Spot---------------------------------------------

Spot<-createSpot(start.date, Today)
Spot<-sqldf("select * from Spot order by Month")
#create spot start prices using the latest monthly prices for all prices
# latest_row<-length(Spot$Month)
# Spot_start<-Spot[latest_row,c(-1,-15)]

#Insert last Spot prices into Access
dbhandle <- odbcDriverConnect('driver={SQL Server};server=ORG130486\\SQLEXPRESS;database=ROil9;trusted_connection=true')
max_month<-max(as.Date(paste("01", tolower(substr(dt$Brent$`CO1 Comdty`$CURRENT_CONTRACT_MONTH_YR,1,3)), 
                             substr(dt$Brent$`CO1 Comdty`$CURRENT_CONTRACT_MONTH_YR,5,6), sep="/"), "%d/%B/%y"))
front_month_Brent<-seq.Date(max_month, by="15 days", length.out = 2)[2]
front_month_AUD<-as.Date(ifelse(day(max_month)<=27, as.Date(paste("27", month(max_month), year(max_month)), "%d%M%Y"), 
                        as.Date(paste("27", month(max_month)+1, year(max_month)), "%d%M%Y")))
front_month_new<-data.frame(date=today(), Front_Month_Brent=front_month_Brent, Front_Month_AUD=front_month_AUD)
sqlSave(dbhandle,front_month_new, "Lookup_Dates", append = T, rownames = F)
data <-sqlQuery(dbhandle, "EXEC sp_Spot_Last")
Spot_start<-data[,c("Brent", "AUD", "JCC", "WTI", "NBP", "AUDNZD", "GBP", "Tapis", "Kutubu", "Gippsland", "Naphtha", "JKM", "Nuclear", "Propane", "HHUSDMMBtu")]
names(Spot_start)[names(Spot_start)=="Propane"]<-"LPG"
names(Spot_start)[names(Spot_start)=="Naphtha"]<-"MOPJ"
Spot_start$NBP<-Spot$NBP[length(Spot$Month)]

 
mean_diff<-mean(Spot$MOPJ-Spot$Brent, na.rm = TRUE)
Spot_start$MOPJ<-((1)/length(Spot_start[,1]))*mean_diff + mean_diff * (length(Spot_start[,1])-1)/length(Spot_start[,1]) + as.numeric(paste(Spot_start$Brent[1]))


# Create master lookup table. sp_Master_Lookup is a stored procedure on SQL server to create query table. 
sqlQuery(dbhandle, "EXEC sp_Master_Lookup") 
closeAllConnections()

connectToAccess<-function(){
    tryCatch(mdbConnect <<- odbcDriverConnect("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//sydnn033/Data/OilModelDatabase/Roil_Database_SQL_Server.accdb"), 
             warning=function(w) {Sys.sleep(5); connectToAccess1();}, error=function(e) {Sys.sleep(5); connectToAccess1();})
}

connectToAccess1<-function(){
    tryCatch(mdbConnect <<- odbcDriverConnect("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//sydnn033/Data/OilModelDatabase/Roil_Database_SQL_Server.accdb"), 
             warning=function(w) {Sys.sleep(5); connectToAccess();}, error=function(e) {Sys.sleep(5); connectToAccess();})
}

tryCatch(mdbConnect <<- odbcDriverConnect("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//sydnn033/Data/OilModelDatabase/Roil_Database_SQL_Server.accdb"), warning=function(w) {Sys.sleep(5); connectToAccess();}, error=function(e) {Sys.sleep(5); connectToAccess();})
sqlSave(mdbConnect, data, "Spot_Last", append=T, rownames = F)
closeAllConnections()

# load LPG retail margin historical from DB
oLPG_Retail_margin_Historical<-LPG_Retail_margin_Historical(start.date, Today)

#   3.2.Forward curves-----------------------------------

FPC<-createFPC(FPC.start.date)
FPC<-FPC[c("Month", "Brent", "AUD", "JCC", "WTI", "NBP", "AUDNZD", "GBP", "Tapis", "Kutubu", "Gippsland", "MOPJ", "JKM", "Nuclear", "HHUSDMMBtu")]

Spot_start$JCC<-FPC$JCC[1]

# Load LPG retail margin forecasts into R
oLPG_Retail_margin_Forecast<-LPG_Retail_margin_Forecast(FPC.start.date, FPC.end.date)

#   3.3.Options Prices-----------------------------------

dbhandle <- odbcDriverConnect('driver={SQL Server};server=ORG130486\\SQLEXPRESS;database=ROil9;trusted_connection=true')
oprices<-sqlQuery(dbhandle, "select * from Master_Lookup")
oprices<-oprices[,24:44]
closeAllConnections()
col_names<-names(oprices)
oprices<-oprices[!is.na(oprices$Month_Number)&(!is.na(oprices[[col_names[7]]])|!is.na(oprices[[col_names[8]]])|!is.na(oprices[[col_names[9]]])|!is.na(oprices[[col_names[10]]])
                 |!is.na(oprices[[col_names[11]]])|!is.na(oprices[[col_names[12]]])|!is.na(oprices[[col_names[13]]])|!is.na(oprices[[col_names[14]]])|!is.na(oprices[[col_names[15]]])
                 |!is.na(oprices[[col_names[16]]])|!is.na(oprices[[col_names[17]]])|!is.na(oprices[[col_names[18]]])|!is.na(oprices[[col_names[19]]])|!is.na(oprices[[col_names[20]]])
                 |!is.na(oprices[[col_names[21]]])),]
oprice<- as.data.frame(t(oprices[c(7:ncol(oprices))])) #prices only (FPC, tickers, dates etc removed)
colnames(oprice)<- t(oprices["contractmonth"])
strike<- as.data.frame(replicate(ncol(oprice),as.numeric(row.names(oprice))/100))

#   3.4.Create Position-----------------------------------

create_position(FPC.start.date, FPC.end.date, start.date, Today)

#4. TIME LINE ----------------------------------------------------------------------------------------------

time_line<- seq.Date(as.Date(FPC.start.date), by = "month", length.out = 36) # Base 1 -> month 1 - 36

#5. SAVE TO AWS S3 -----------------------------------------------------------------------------------------

basePath <- PathName$new("RDS_Repository", "Input",Ndir)
Bucket<-"origin-risk-data"
dataLoader = DataLoader$new(Bucket, cache = FALSE)

# Prices
dataLoader$save(Spot, as = basePath$path("Spot.rds"))
dataLoader$save(FPC, as = basePath$path("FPC.rds"))
dataLoader$save(oLPG_Retail_margin_Historical, as = basePath$path("LPG_Retail_margin_Historical.rds"))
dataLoader$save(oLPG_Retail_margin_Forecast, as = basePath$path("LPG_Retail_margin_Forecast.rds"))
dataLoader$save(oprice, as = basePath$path("oprice.rds"))
dataLoader$save(time_line, as = basePath$path("time_line.rds"))
dataLoader$save(strike, as = basePath$path("strike.rds"))
dataLoader$save(oprices, as = basePath$path("oprices.rds"))
dataLoader$save(Spot_start, as = basePath$path("Spot_start.rds"))

# Position
dataLoader$save(position_forecast, as = basePath$path("position_forecast.rds"))
dataLoader$save(position_hedge, as = basePath$path("position_hedge.rds"))
dataLoader$save(position_pricing, as = basePath$path("position_pricing.rds"))
dataLoader$save(position_volumetric, as = basePath$path("position_volumetric.rds"))
dataLoader$save(position_OMT_Volume, as = basePath$path("position_OMT_Volume.rds"))
dataLoader$save(position_actual, as = basePath$path("position_actual.rds"))

#record date of creation

dataLoader$save(Ndir, as = "RDS_Repository/NdirInput.rds")

# 6. Save CSV files to Source Data/Volumes Data folder-------------------------------------------

setwd("//sydnn033/Data/OilModelCSV")

#volumes

PF_name<-names(position_forecast)

lapply(PF_name,function(x){
    tdat<-position_forecast[[x]]

    fname<-gsub("\\s+","_",x)
    fname<-gsub("(\\[|\\]|/)","",fname)

    #Risk & IG
    if(fname%in%c("648620_Forecast","APLNG_Forecast","LNG_Portfolio_Volume","LNG_Portfolio_Volume_Spot_Leg","LPG_Upstream_Forecast","Upstream_Oil_Forecast")){

        write.csv(tdat,file=paste0("OilPositionRisk/Volumes_Data/",fname,".csv"),row.names=FALSE)
        write.csv(tdat,file=paste0("OilPositionIG/Volumes_Data/",fname,".csv"),row.names=FALSE)

    }

    #Risk, EM & PD
    if(fname%in%c("EM_Gas_Forecast","LPG_Retail_Forecast")){

        write.csv(tdat,file=paste0("OilPositionRisk/Volumes_Data/",fname,".csv"),row.names=FALSE)
        write.csv(tdat,file=paste0("OilPositionEM/Volumes_Data/",fname,".csv"),row.names=FALSE)
        write.csv(tdat,file=paste0("OilPositionPD/Volumes_Data/",fname,".csv"),row.names=FALSE)

    }

})


#hedges
PF_name<-names(position_hedge)

lapply(PF_name,function(x){
    tdat<-position_hedge[[x]]

    fname<-gsub("\\s+","_",x)
    fname<-gsub("(\\[|\\]|/)","",fname)


    if(!fname%in%c("Oil_Hedges")){

        if(fname%in%c("Oil_Hedges_Pivot")){

            #Risk 
            write.csv(tdat,file=paste0("OilPositionRisk/Hedges_Data/",fname,".csv"),row.names=FALSE)

            #EM & PD
            tdat1<-tdat[which(tdat$Book=="EM"),]
            write.csv(tdat1,file=paste0("OilPositionPD/Hedges_Data/",fname,".csv"),row.names=FALSE)
            write.csv(tdat1,file=paste0("OilPositionEM/Hedges_Data/",fname,".csv"),row.names=FALSE)

            #IG
            tdat2<-tdat[which(tdat$Book=="APLNG"),]
            write.csv(tdat2,file=paste0("OilPositionIG/Hedges_Data/",fname,".csv"),row.names=FALSE)
        }


        if(fname%in%c("FX_Hedges")){
            
            write.csv(tdat,file=paste0("OilPositionRisk/Hedges_Data/",fname,".csv"),row.names=FALSE)
            write.csv(tdat,file=paste0("OilPositionPD/Hedges_Data/",fname,".csv"),row.names=FALSE)
            write.csv(tdat,file=paste0("OilPositionEM/Hedges_Data/",fname,".csv"),row.names=FALSE)
            write.csv(tdat,file=paste0("OilPositionIG/Hedges_Data/",fname,".csv"),row.names=FALSE)
        }

        if(!fname%in%c("Oil_Hedges_Pivot","FX_Hedges")){
            
            write.csv(tdat,file=paste0("OilPositionRisk/Hedges_Data/",fname,".csv"),row.names=FALSE)
            write.csv(tdat,file=paste0("OilPositionIG/Hedges_Data/",fname,".csv"),row.names=FALSE)
            
        }

    }

})



quit(save="no")



