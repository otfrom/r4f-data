(ns r4f-data.etl-test
  (:use midje.sweet
        midje.cascalog
        r4f-data.etl))

(def device-data
  ["{"
   "\"32626630653634622d666530652d343537382d626362392d306265666465613034336661\": [],"
   "\"38373239633161372d373238632d346636342d626636392d393633346431373931306238\": [[\"completenessRatios\",\"[]\",1358913726132002], [\"description\",\"Ext Power indication\",1358913726131003], [\"deviceId\",\"8729c1a7-728c-4f64-bf69-9634d17910b8\",1358913726131000], [\"entityId\",\"4e558c03-b10e-4630-a11a-671c48516621\",1358913726131002], [\"location\",\"{\\\"name\\\":\\\"36 Geddes Road, Dining/Living Room\\\"}\",1358913726132000], [\"metadata\",\"{\\\"customer_ref\\\":\\\"8\\\"}\",1358913726131004], [\"parentId\",\"\",1358913726131001], [\"privacy\",\"private\",1358913726131005], [\"readings\",\"[{\\\"type\\\":\\\"status\\\",\\\"unit\\\":\\\"0/1\\\",\\\"resolution\\\":0,\\\"accuracy\\\":0.0,\\\"period\\\":\\\"INSTANT\\\",\\\"min\\\":0.0,\\\"max\\\":1.0,\\\"correction\\\":false,\\\"correctedUnit\\\":\\\"\\\",\\\"correctionFactorBreakdown\\\":\\\"\\\",\\\"completenessRatios\\\":[]}]\",1358913726132001]],"
   "\"64343738393765322d613966352d346430332d626463372d646235356632303033323238\": [],"
   "\"63633063336162382d613330362d343434622d393335372d613662393531383764366334\": [[\"deviceId\",\"cc0c3ab8-a306-444b-9357-a6b95187d6c4\",1349686153627000], [\"location\",\"{}\",1294325411560], [\"metadata\",\"{\\\"serial_no\\\":\\\"09120702\\\",\\\"mpid\\\":5724}\",1342610812222000], [\"privacy\",\"private\",1294325411560], [\"qualifiedMeteringPointId\",\"MPAN.1411960830008\",1294325411560], [\"readings\",\"[{\\\"type\\\":\\\"electricityConsumption\\\",\\\"unit\\\":\\\"kWh\\\",\\\"resolution\\\":0.0,\\\"accuracy\\\":0.0,\\\"period\\\":\\\"instant\\\"}]\",1294325411560]],"])


(fact
 (split-sstable-row (second device-data)) => ["32626630653634622d666530652d343537382d626362392d306265666465613034336661" "[],"]
 (split-sstable-row (nth device-data 2)) => ["38373239633161372d373238632d346636342d626636392d393633346431373931306238" "[[\"completenessRatios\",\"[]\",1358913726132002], [\"description\",\"Ext Power indication\",1358913726131003], [\"deviceId\",\"8729c1a7-728c-4f64-bf69-9634d17910b8\",1358913726131000], [\"entityId\",\"4e558c03-b10e-4630-a11a-671c48516621\",1358913726131002], [\"location\",\"{\\\"name\\\":\\\"36 Geddes Road, Dining/Living Room\\\"}\",1358913726132000], [\"metadata\",\"{\\\"customer_ref\\\":\\\"8\\\"}\",1358913726131004], [\"parentId\",\"\",1358913726131001], [\"privacy\",\"private\",1358913726131005], [\"readings\",\"[{\\\"type\\\":\\\"status\\\",\\\"unit\\\":\\\"0/1\\\",\\\"resolution\\\":0,\\\"accuracy\\\":0.0,\\\"period\\\":\\\"INSTANT\\\",\\\"min\\\":0.0,\\\"max\\\":1.0,\\\"correction\\\":false,\\\"correctedUnit\\\":\\\"\\\",\\\"correctionFactorBreakdown\\\":\\\"\\\",\\\"completenessRatios\\\":[]}]\",1358913726132001]],"])

(fact
 (unhexify "32626630653634622d666530652d343537382d626362392d306265666465613034336661") => "2bf0e64b-fe0e-4578-bcb9-0befdea043fa")

(fact
 (hexify "2bf0e64b-fe0e-4578-bcb9-0befdea043fa") => "32626630653634622d666530652d343537382d626362392d306265666465613034336661")

(fact 
 (map data-line? device-data) => '(false true true true true))