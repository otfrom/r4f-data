(ns r4f-data.retrofit-data-test
  (:require [cheshire.core :as json]
            [clj-time.core :as time])
  (:use midje.sweet
        midje.cascalog
        r4f-data.retrofit-data))

(def m7-data [["cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:00:00+0000" 203127]
              ["cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:05:00+0000" 203127]
              ["cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:10:00+0000" 203127]
              ["cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:15:00+0000" 203127]
              ["cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:20:00+0000" 203127]
              ["cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:25:00+0000" 203127]])

(def devices-data [["cc0c3ab8-a306-444b-9357-a6b95187d6c4:status" "cc0c3ab8-a306-444b-9357-a6b95187d6c4" "4e558c03-b10e-4630-a11a-671c48516621" "status" "0/1" "INSTANT"]
                   ["cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "cc0c3ab8-a306-444b-9357-a6b95187d6c4" "4e558c03-b10e-4630-a11a-671c48516621" "electricityConsumption" "kWh" "instant"]])

(def project-data
  [["381" "Gentoo Retrofit Bid" "4e558c03-b10e-4630-a11a-671c48516621" "TSB081" "SR4 XXX"]
   ["381" "Gentoo Retrofit Bid" "e830e7a0-6784-4c42-a767-1912b75645a1" "TSB082" "SR5 XXX"]
   ["385" "2050 Now! Achieving the standards for 2050, and the potential role for innovative heating solutions in thermally improved homes" "857acda9-0956-4c3f-8c42-1aa3870ea45d" "TSB121" "NG19 XXX"]])


(fact
 (retrofit-data m7-data devices-data project-data (cascalog.api/stdout)) =>
 (produces
  [["TSB081" "4e558c03-b10e-4630-a11a-671c48516621" "cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:00:00+0000" 203127 "cc0c3ab8-a306-444b-9357-a6b95187d6c4" "electricityConsumption" "kWh" "instant"]
   ["TSB081" "4e558c03-b10e-4630-a11a-671c48516621" "cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:05:00+0000" 203127 "cc0c3ab8-a306-444b-9357-a6b95187d6c4" "electricityConsumption" "kWh" "instant"]
   ["TSB081" "4e558c03-b10e-4630-a11a-671c48516621" "cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:10:00+0000" 203127 "cc0c3ab8-a306-444b-9357-a6b95187d6c4" "electricityConsumption" "kWh" "instant"]
   ["TSB081" "4e558c03-b10e-4630-a11a-671c48516621" "cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:15:00+0000" 203127 "cc0c3ab8-a306-444b-9357-a6b95187d6c4" "electricityConsumption" "kWh" "instant"]
   ["TSB081" "4e558c03-b10e-4630-a11a-671c48516621" "cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:20:00+0000" 203127 "cc0c3ab8-a306-444b-9357-a6b95187d6c4" "electricityConsumption" "kWh" "instant"]
   ["TSB081" "4e558c03-b10e-4630-a11a-671c48516621" "cc0c3ab8-a306-444b-9357-a6b95187d6c4:electricityConsumption" "2012-02-01T00:25:00+0000" 203127 "cc0c3ab8-a306-444b-9357-a6b95187d6c4" "electricityConsumption" "kWh" "instant"]]))

(fact
 (on-day? (time/date-time 2012 01 02 0 0 0 0) 2012 01 01) => false
 (on-day? (time/date-time 2012 01 01 0 0 0 0) 2012 01 01) => true
 (on-day? (time/date-time 2012 01 01 23 59 59 999) 2012 01 01) => true)

(fact
 ;; "64a602e7-9558-4693-a2b2-b7cbc677b957:electricityConsumption" on 2011-12-30"))
 (clean-data? "TSB001" "64a602e7-9558-4693-a2b2-b7cbc677b957:electricityConsumption" "2011-12-30T00:25:00+0000") => false
 ;; ede06768-59d1-4bf5-b21e-51b1b8b29106:electricityConsumption on c("2012-01-11","2012-01-12","2012-01-13","2012-01-14"))
 (clean-data? "TSB001" "ede06768-59d1-4bf5-b21e-51b1b8b29106:electricityConsumption" "2012-01-11T00:25:00+0000") => false
 (clean-data? "TSB001" "ede06768-59d1-4bf5-b21e-51b1b8b29106:electricityConsumption" "2012-01-12T00:25:00+0000") => false
 (clean-data? "TSB001" "ede06768-59d1-4bf5-b21e-51b1b8b29106:electricityConsumption" "2012-01-13T00:25:00+0000") => false
 (clean-data? "TSB001" "ede06768-59d1-4bf5-b21e-51b1b8b29106:electricityConsumption" "2012-01-14T00:25:00+0000") => false
 
 ;; ede06768-59d1-4bf5-b21e-51b1b8b29106:electricityConsumption on 2011-12-31"))
 (clean-data? "TSB001" "ede06768-59d1-4bf5-b21e-51b1b8b29106:electricityConsumption" "2011-12-31T00:25:00+0000") => false
 
 ;; - remove 2012-11-29 for TSB001
 (clean-data? "TSB001" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2012-11-29T00:25:00+0000") => false
 
 ;; - manually remove bad data for specific properties and date ranges 
 ;; # Manually remove bad data for TSB025 pre May 2011
 ;; try(all.data.usable<-subset(all.data.usable,property!="TSB025" | date>=as.Date("2011-05-01")))
 (clean-data? "TSB025" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2011-05-01T00:25:00+0000") => false
 (clean-data? "TSB025" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "1911-05-01T00:25:00+0000") => false
 (clean-data? "TSB025" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2011-05-02T00:25:00+0000") => true

 ;; # Manually remove bad data for TSB085 pre 2011
 ;; try(all.data.usable<-subset(all.data.usable,property!="TSB085" | date>=as.Date("2011-01-01")))
 (clean-data? "TSB085" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2010-05-02T00:25:00+0000") => false
 (clean-data? "TSB085" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2011-05-02T00:25:00+0000") => true

 ;; # Manually remove bad data for TSB044 pre 2011
 ;; try(all.data.usable<-subset(all.data.usable,property!="TSB044" | date>=as.Date("2011-09-01")))
 (clean-data? "TSB044" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2010-05-02T00:25:00+0000") => false
 (clean-data? "TSB044" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2011-05-02T00:25:00+0000") => true
 
 ;; # Manually remove bad data for TSB118 pre 2012
 ;; try(all.data.usable<-subset(all.data.usable,property!="TSB118" | date>=as.Date("2012-01-01")))
 (clean-data? "TSB118" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2011-05-02T00:25:00+0000") => false
 (clean-data? "TSB118" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2012-05-02T00:25:00+0000") => true

 ;; # Manually remove bad meter for TSB024
 ;; all.data.usable<-subset(all.data.usable,metercode!="6ae7adc3-7bdb-41cb-9492-a87a999ea5e0:gasConsumption")
 (clean-data? "TSB024" "6ae7adc3-7bdb-41cb-9492-a87a999ea5e0:gasConsumption" "2012-05-02T00:25:00+0000") => true

 ;; # Manually remove bad data for TSB052
 ;; try(all.data.usable<-subset(all.data.usable,property!="TSB052" | date>=as.Date("2011-12-01")))
 (clean-data? "TSB052" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2011-11-30T00:25:00+0000") => false
 (clean-data? "TSB052" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2011-12-01T00:25:00+0000") => true

 ;; # Manually remove bad data for TSB106
 ;; try(all.data.usable<-subset(all.data.usable,property!="TSB106" | date<=as.Date("2012-09-01")))
 (clean-data? "TSB016" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2012-08-30T00:25:00+0000") => true
 (clean-data? "TSB016" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2012-09-01T00:25:00+0000") => false

 ;; # Manually remove bad data for TSB121
 ;; try(all.data.usable<-subset(all.data.usable,property!="TSB121" | date>=as.Date("2011-05-01")))
 (clean-data? "TSB121" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2011-04-30T00:25:00+0000") => false
 (clean-data? "TSB121" "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption" "2011-05-01T00:25:00+0000") => true

 ;; - manually remove meter c01ecdcf-443d-4ed0-91a0-2d12dc0a2c71:electricityConsumption after 2012-02-01
 (clean-data? "TSB121" "c01ecdcf-443d-4ed0-91a0-2d12dc0a2c71:electricityConsumption" "2012-02-01T00:25:00+0000") => false
 (clean-data? "TSB121" "c01ecdcf-443d-4ed0-91a0-2d12dc0a2c71:electricityConsumption" "2012-01-31T00:25:00+0000") => true

 ;; - manually remove meter 6e10b3b6-dcdd-40f0-8198-55706fb7c726:electricityConsumption before and including date 2012-02-01
 (clean-data? "TSB121" "6e10b3b6-dcdd-40f0-8198-55706fb7c726:electricityConsumption" "2012-02-02T00:25:00+0000") => true
 (clean-data? "TSB121" "6e10b3b6-dcdd-40f0-8198-55706fb7c726:electricityConsumption" "2012-02-01T00:25:00+0000") => false

 ;; - manually remove meter 662d7079-0604-4bd6-9ae5-dce0312bb35a:gasConsumption
 (clean-data? "TSB121" "662d7079-0604-4bd6-9ae5-dce0312bb35a:gasConsumption" "2038-02-01T00:25:00+0000") => false)