# Switches to control which sections of code will run
process.data<-0 # On/off switch for reading all the data
clean.data<-1 # On/off switch for running data processing - e.g. differencing cumulative meters

# Load libraries
require(ggplot2)
require(reshape)
require(zoo)
setwd("/Users/francinebennett/Desktop/analysis/tsb-embed/")

# Read in dataset for areas
areas<-read.csv("property_areas.csv")

# Load in all raw files and combine to a single data frame.
## For pulse meters, daily reading is the sum of 5-min readings
## For cumulative meters, daily reading is the day's latest (max) reading

if (process.data == 1) {
  all.files<-list.files(path = "/Volumes/diskbox/retrofit_datadump/", pattern = NULL, all.files = FALSE,                        
                        full.names = FALSE, recursive = TRUE,
                        ignore.case = FALSE, include.dirs = FALSE)
  all.data<-data.frame(matrix(nrow=0,ncol=8))
  print("reading files")
  for (i in 1:length(all.files)){
    temp<-read.csv(paste("/Volumes/diskbox/retrofit_datadump/",all.files[i],sep=""),sep="\t",header=FALSE)
    temp$V4<-gsub("T"," ",temp$V4)
    temp$V4<-as.POSIXlt(temp$V4)
    temp$date<-as.Date(temp$V4)
    if ((unique(temp$V7)) %in% c("ElecMeterPulse","GasMeterPulse")) {
      temp<-aggregate(temp$V5,by=list(temp$V1,temp$V2,temp$V3,temp$V6,temp$V7,temp$V8,temp$date),FUN=sum)
    } else
    {
      temp<-aggregate(temp$V5,by=list(temp$V1,temp$V2,temp$V3,temp$V6,temp$V7,temp$V8,temp$date),FUN=max)
      }
    #print(all.files[i])
    all.data<-rbind(temp,all.data)
  }
  print("finished reading files")
  names(all.data)<-c("property","property_reference","metercode","unit","type","cumulative_instant","date","consumption")
  write.csv(all.data,"all_data.csv",row.names=FALSE)
}

# Apply further cleaning to data
if (clean.data == 1) {
  all.data<-read.csv("all_data.csv",as.is=TRUE)
  all.data$date<-as.Date(all.data$date)
  
  # Make sure all dates are covered - add blank rows for missing dates
  meter.names<-unique(all.data[,c("metercode","property","property_reference","unit","type","cumulative_instant")])
  dates<-as.data.frame(seq(from=as.Date("2010-04-21"),to=as.Date("2013-04-01"),by=1))
  dates<-expand.grid.df(dates,meter.names)
  names(dates)<-c("date","metercode","property","property_reference","unit","type","cumulative_instant")
  all.data<-merge(dates,all.data,all.x=TRUE)
  rm(dates)
  
  # Backfill straight line readings for cumulative meters which have gaps short enough to safely interpolate 
  backfill.meters<-c(
    "e75fb81b-814a-414c-95ea-a5f65e36fea4:electricityConsumption",
    "8f5c55cc-88ef-4927-b7ca-dec9c21182b6:gasAsHeatingFuel",      
    "4529ef6e-09a8-4fd8-b881-fb418ff3468d:electricityConsumption",
    "dda1d834-add2-4219-a467-956f945c4414:electricityConsumption",
    "7b888fc4-7ae0-422f-827d-e0fb5d9065fe:electricityConsumption",
    "344aa09b-544e-47b7-852a-5019754b2193:gasAsHeatingFuel",      
    "ae9d7a7a-9ce7-486e-be4c-be6f930db1ba:electricityConsumption",
    "6b545660-1cdd-4177-9bad-dde44d0f22c7:electricityConsumption",
    "ede06768-59d1-4bf5-b21e-51b1b8b29106:electricityConsumption",
    "64a602e7-9558-4693-a2b2-b7cbc677b957:electricityConsumption")
  
  backfilled.data<-data.frame(matrix(nrow=0,ncol=9))
  
  print("fixing backfilled meters")
  for (i in 1:length(backfill.meters)){
    #print(backfill.meters[i])  
    a<-subset(all.data,metercode==backfill.meters[i])
    b<-subset(a,!is.na(consumption))
    start.date<-min(b$date)
    end.date<-max(b$date)
    a<-a[order(a$date),]
    a<-subset(a,date>=start.date & date<=end.date)
    a$backfilled_consumption<-na.approx(a$consumption,na.rm=FALSE)
    backfilled.data<-rbind(backfilled.data,a)
  }
  backfilled.data<-backfilled.data[,c("date","metercode","backfilled_consumption")]
  all.data<-merge(all.data,backfilled.data,all.x=TRUE)
  all.data[is.na(all.data$backfilled_consumption),]$backfilled_consumption<-all.data[is.na(all.data$backfilled_consumption),]$consumption
  meter.list<-unique(all.data$metercode)
  
  ## Create differenced data for cumulative meters
  all.data.diff<-data.frame(matrix(nrow=0,ncol=4))
  
  print("differencing cumulative data")
  for (i in 1:length(meter.list)){
    #print(meter.list[i])
    temp<-subset(all.data,metercode==meter.list[i])[,c("date","metercode","backfilled_consumption")]
    temp<-temp[order(temp$date),]
    temp$diff<-c(NA,diff(temp$backfilled_consumption,1))
    all.data.diff<-rbind(temp,all.data.diff)
  }
  names(all.data.diff)<-c("date","metercode","backfilled_consumption","diff")
  
  all.data.totals<-merge(all.data,all.data.diff,all.x=TRUE)
  
  # late removal of meters submitting incorrect data
  all.data.totals<-subset(all.data.totals,!metercode %in% 
                            c("6ae7adc3-7bdb-41cb-9492-a87a999ea5e0:gasConsumption",
                              "d8d1f13d-2287-4f8f-addf-a9c73780136d:electricityConsumption",
                              "fb18d9ff-2f01-432e-af92-1a9961f8a3fb:gasAsHeatingFuel") )
    
  all.data.totals<-all.data.totals[order(all.data.totals$property,decreasing=TRUE),]
  try(all.data.totals[all.data.totals$type %in% c("GasMeterPulse","ElecMeterPulse"),]$diff<-
    all.data.totals[all.data.totals$type %in% c("GasMeterPulse","ElecMeterPulse"),]$backfilled_consumption)
  try(all.data.totals[all.data.totals$diff<0 & !is.na(all.data.totals$diff),]$diff<-NA)
  write.csv(all.data.totals,"all_data_totals_ecobuild.csv",row.names=FALSE)
}

all.data.totals<-read.csv("all_data_totals_ecobuild.csv",as.is=TRUE)

# Set CO2 emissions factors and power prices 
electricity.factor<-0.422 # Mains Electricity  0.422 kgCO2e/kWh
gas.factor<-0.194 # Mains Gas 0.194 kgCO2e/kWh
gas.volume.factor<-2.125 # Per cubic metre kgCO2e/kWh
electricity.price<-0.1448
gas.price.kwh<-0.0443
gas.price.volume<-gas.price.kwh*(gas.volume.factor/gas.factor)

# Primary energy factors to calculate how many kWh generated to allow this consumption
electric.pef=2.8
gas.pef=1.0
gas.pef.volume=1.0*(gas.volume.factor/gas.factor)

# Convert everything to kwh for energy per area normalisation
electric.multiplier<-1
gas.volume.multiplier<-gas.volume.factor/gas.factor

energy.factors<-as.data.frame(rbind(
  c("electricityConsumption","kWh",electricity.factor,electricity.price,electric.pef,electric.multiplier),
  c("electricityConsumption","Wh",electricity.factor/1000,electricity.price/1000,electric.pef/1000,electric.multiplier/1000),
  c("electricityConsumption","undeclared",electricity.factor,electricity.price,electric.pef,electric.multiplier),
  c("gasAsHeatingFuel","m^3",gas.volume.factor,gas.price.volume,gas.pef.volume,gas.volume.multiplier),
  c("gasConsumption","m^3",gas.volume.factor,gas.price.volume,gas.pef.volume,gas.volume.multiplier),
  c("heatConsumption","kWh",electricity.factor,electricity.price,gas.pef,electric.multiplier),
  c("ElecMeterPulse","Wh",electricity.factor/1000,electricity.price/1000,electric.pef/1000,electric.multiplier/1000),
  c("GasMeterPulse","per m Cube",gas.volume.factor,gas.price.volume,gas.pef.volume,gas.volume.multiplier),
  c("Electrical","kWh",electricity.factor,electricity.price,electric.pef,electric.multiplier),
  c("Gas_Usage","kWh",gas.factor,gas.price.kwh,gas.pef,electric.multiplier),
  c("Gas_Usage","m^3",gas.volume.factor,gas.price.volume,gas.pef.volume,gas.volume.multiplier)
),stringsAsFactors=FALSE)
names(energy.factors)<-c("type","unit","factor","price","pef","kwh_conversion")
energy.factors$factor<-as.numeric(energy.factors$factor)
energy.factors$price<-as.numeric(energy.factors$price)
energy.factors$pef<-as.numeric(energy.factors$pef)
energy.factors$kwh_conversion<-as.numeric(energy.factors$kwh_conversion)
all.data.totals$date<-as.Date(all.data.totals$date)

# Add friendly meter title
all.data.totals$title<-paste(all.data.totals$type,all.data.totals$unit,substr(all.data.totals$metercode,0,8),sep=" ")

# Calculate annualised emissions and costs per meter
all.data.totals<-merge(all.data.totals,energy.factors,all.x=TRUE)
all.data.totals$property_num<-as.numeric(substr(all.data.totals$property,4,6))
all.data.totals<-merge(all.data.totals,areas)
all.data.totals$annualised_emissions<-all.data.totals$diff*all.data.totals$factor*365.25/all.data.totals$area
all.data.totals$normalised_cost<-all.data.totals$diff*all.data.totals$price*365.25/all.data.totals$area
all.data.totals$annual_cost<-all.data.totals$diff*all.data.totals$price*365.25
all.data.totals$primary_energy_consumption<-all.data.totals$diff*all.data.totals$pef*365.25/all.data.totals$area
all.data.totals$energy_per_area<-all.data.totals$diff*all.data.totals$kwh_conversion*365.25/all.data.totals$area
all.data.totals$is_primary<-"primary"

# Find a first date per consumption meter where it's providing non-zero readings, and make emissions NA before that
temp<-subset(all.data.totals,!is.na(diff))
temp$date<-as.Date(temp$date)
start.dates<-aggregate(temp$date,by=list(temp$metercode),FUN=min)
names(start.dates)<-c("metercode","start_date")
all.data.totals<-merge(all.data.totals,start.dates,all.x=TRUE)
try(all.data.totals[all.data.totals$date<all.data.totals$start_date & all.data.totals$is_primary=="primary",]$annualised_emissions<-NA)

# Add up emissions from consumption per day with non-na readings from all primary consumption meters
all.data.consumption<-subset(all.data.totals,is_primary=="primary")
all.data.consumption.day<-aggregate(all.data.consumption[,c("annualised_emissions","normalised_cost","annual_cost","primary_energy_consumption","energy_per_area")],by=list(all.data.consumption$date,all.data.consumption$property),FUN=sum)
names(all.data.consumption.day)<-c("date","property","annualised_emissions","normalised_cost","annual_cost","primary_energy_consumption","energy_per_area")

# Remove initial periods with almost-zero readings
temp<-subset(all.data.consumption.day,annualised_emissions>2)
new.start.dates<-aggregate(temp$date,by=list(temp$property),FUN=min)
names(new.start.dates)<-c("property","start.date")
all.data.consumption.day<-merge(all.data.consumption.day,new.start.dates,all.x=TRUE)
all.data.consumption.initial<-subset(all.data.consumption.day,date<start.date)
all.data.consumption.initial<-subset(all.data.consumption.initial,!is.na(annualised_emissions))
all.data.consumption.day<-subset(all.data.consumption.day,date>=start.date)

# Calculate annualised emissions per property - average by day of year, then average across those
all.data.consumption.day<-subset(all.data.consumption.day,!is.na(annualised_emissions))
all.data.consumption.day$dayofyear<-strftime(all.data.consumption.day$date, format = "%j")
temp<-aggregate(all.data.consumption.day[,c("annualised_emissions","normalised_cost","annual_cost","primary_energy_consumption","energy_per_area")],by=list(all.data.consumption.day$dayofyear,all.data.consumption.day$property),FUN=mean)
emissions<-aggregate(temp[,c("annualised_emissions","normalised_cost","annual_cost","primary_energy_consumption","energy_per_area")],by=list(temp$Group.2),FUN=mean)
names(emissions)<-c("property","annual_emissions","normalised_cost","annual_cost","primary_energy_consumption","energy_per_area")
emissions$property<-as.character(emissions$property)
emissions<-emissions[order(emissions$annual_emissions),]
emissions$property<-factor(emissions$property,emissions$property)
write.table(emissions[,c("property","annual_emissions","normalised_cost","annual_cost","primary_energy_consumption","energy_per_area")],"ecobuild_emissions.csv",row.names=FALSE,sep="\t")

# Add per meter back on to get electrical / gas emissions split
all.data.consumption$electric_or_gas<-"Gas"
all.data.consumption[grep("electric",tolower(all.data.consumption$type)),]$electric_or_gas<-"Electrical"

all.data.consumption.split<-aggregate(all.data.consumption[,c("annualised_emissions","energy_per_area","normalised_cost","annual_cost","primary_energy_consumption")],by=list(all.data.consumption$property,all.data.consumption$date,all.data.consumption$electric_or_gas),FUN=sum)
names(all.data.consumption.split)[1]<-"property"
names(all.data.consumption.split)[2]<-"date"
names(all.data.consumption.split)[3]<-"electric_or_gas"
t<-cast(all.data.consumption.split,property+date~electric_or_gas,value="annualised_emissions")
names(t)<-c("property","date","electrical_emissions","gas_emissions")
s<-cast(all.data.consumption.split,property+date~electric_or_gas,value="energy_per_area")
names(s)<-c("property","date","electrical_per_area","gas_per_area")
r<-cast(all.data.consumption.split,property+date~electric_or_gas,value="normalised_cost")
names(r)<-c("property","date","electrical_normalised_cost","gas_normalised_cost")
p<-cast(all.data.consumption.split,property+date~electric_or_gas,value="annual_cost")
names(p)<-c("property","date","electrical_annual_cost","gas_annual_cost")
q<-cast(all.data.consumption.split,property+date~electric_or_gas,value="primary_energy_consumption")
names(q)<-c("property","date","electrical_primary_energy_consumption","gas_primary_energy_consumption")

all.data.consumption.day<-merge(all.data.consumption.day,t,all.x=TRUE)
all.data.consumption.day<-merge(all.data.consumption.day,s,all.x=TRUE)
all.data.consumption.day<-merge(all.data.consumption.day,r,all.x=TRUE)
all.data.consumption.day<-merge(all.data.consumption.day,p,all.x=TRUE)
all.data.consumption.day<-merge(all.data.consumption.day,q,all.x=TRUE)

temp<-aggregate(all.data.consumption.day[,c("electrical_emissions","gas_emissions","electrical_per_area","gas_per_area",
                                            "electrical_normalised_cost","gas_normalised_cost",
                                            "electrical_annual_cost","gas_annual_cost",
                                            "electrical_primary_energy_consumption","gas_primary_energy_consumption")],by=list(all.data.consumption.day$dayofyear,all.data.consumption.day$property),FUN=mean,na.rm=TRUE)
temp[is.na(temp)]<-0
split_emissions<-aggregate(temp[,c("electrical_emissions","gas_emissions","electrical_per_area","gas_per_area",
                                   "electrical_normalised_cost","gas_normalised_cost",
                                   "electrical_annual_cost","gas_annual_cost",
                                   "electrical_primary_energy_consumption","gas_primary_energy_consumption")],by=list(temp$Group.2),FUN=mean)
names(split_emissions)<-c("property","electrical_emissions","gas_emissions","electrical_per_area","gas_per_area",
                          "electrical_normalised_cost","gas_normalised_cost",
                          "electrical_annual_cost","gas_annual_cost","electrical_primary_energy_consumption","gas_primary_energy_consumption")
emissions<-merge(emissions,split_emissions,all.x=TRUE)

# Write final results to file
write.table(emissions,"ecobuild_emissions.csv",row.names=FALSE,sep="\t")