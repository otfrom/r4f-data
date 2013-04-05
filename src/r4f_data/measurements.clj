(ns r4f-data.measurements
  (:require [r4f-data.etl :as etl]
            [clojure.string :as string]
            [cheshire.core :as json])
  (:use cascalog.api
        clojure.tools.logging))

(defn non-aggregation? [^String line]
  (let [len (.length line)]
    (and (> len 100)
         (= -1 (.indexOf (.substring line 0 (min len 300)) "=>")))))

(defn errored-measurement? [[_ value _]]
  (try
    (.startsWith (str value) "error")
    (catch Exception e
      (errorf e "This shouldn't happen with str of %s" (str value)))))

(defmapcatop verticalize-measurement-rows [^String row]
  (->> (json/parse-string row)
       (remove errored-measurement?)
       (map (fn [[m-tstamp value _]]
              [m-tstamp (-> (json/parse-string value) (get "v"))]))))

;; and only do the json parsing on the individual elements after splitting the huge string
(defmapcatop breakout-readings [^String row]
  (infof "Attempting to get readings out of a %s length row." (.length row))
  (re-seq #"\[[^\]\[]*\]" row))

(defn reading-data [reading-str]
  (let [[tstamp value _] (json/parse-string reading-str)]
    [tstamp (-> (json/parse-string value) (get "v"))]))

(defn non-error-reading? [^String reading]
  (= -1 (.indexOf reading "error")))

(defn sensor-has-data? [^String data-json]
  (> (.length data-json) 3))

(def interesting-sensors
  #{"0cc8989a-9cc7-40d5-97f2-11b34792977c:ElecMeterPulse"
    "b4f58930-cdfa-4b9d-b9a3-6bec7331c6ee:Electrical"
    "4a26f3eb-3ef8-4877-a4a1-d61c55debf06:Electrical"
    "212d5156-1d42-41c8-8fb6-8f4c33a15070:Electrical"
    "a4655a66-ddd5-4e33-a173-c1763495190a:Electrical"
    "9ba51f93-32b3-4246-b6b6-a44930d9d829:GasMeterPulse"
    "39176c26-9662-47fb-a37b-6c6abe2a9f72:Gas Usage"
    "921d084d-8c10-4fd6-aeb9-658ca8f1fe80:Gas Usage"
    "282db7ae-aa42-4d77-a33b-80ff56c52709:Gas Usage"
    "d437a5c9-7f67-4d7d-9806-859830c7fd56:electricityConsumption"
    "0ff35ec1-2622-4d5c-8fbc-a435170ec136:electricityConsumption"
    "9cb977d9-7f7b-486c-9ea2-ee29e695dc62:electricityConsumption"
    "64a602e7-9558-4693-a2b2-b7cbc677b957:electricityConsumption"
    "4529ef6e-09a8-4fd8-b881-fb418ff3468d:electricityConsumption"
    "028630f9-fcc8-403c-aa47-eea647cead10:electricityConsumption"
    "6b545660-1cdd-4177-9bad-dde44d0f22c7:electricityConsumption"
    "ae9d7a7a-9ce7-486e-be4c-be6f930db1ba:electricityConsumption"
    "dbe5057f-5f80-4ea6-990d-474648f3add7:electricityConsumption"
    "3e2c8da2-6571-48a2-a859-83fcf4a59963:electricityConsumption"
    "88ffef81-f839-44b4-9421-827741cbc3b9:electricityConsumption"
    "ede06768-59d1-4bf5-b21e-51b1b8b29106:electricityConsumption"
    "731202f5-db2a-4f14-8451-0931cc7ddc61:electricityConsumption"
    "baf3227c-f20b-4b73-84a6-1c77b7f60cee:electricityConsumption"
    "dda1d834-add2-4219-a467-956f945c4414:electricityConsumption"
    "36e4ed3f-893a-4017-a713-607058d226c5:electricityConsumption"
    "c01ecdcf-443d-4ed0-91a0-2d12dc0a2c71:electricityConsumption"
    "8f1be73c-05ac-47d3-a44a-16cdf3519d06:electricityConsumption"
    "18103be2-114a-4e33-a335-161650b77f5c:electricityConsumption"
    "ebe0eb55-56af-4a75-9820-a200db85fb36:electricityConsumption"
    "b4e59411-7dfa-410d-8c30-f8582f2ed1a4:electricityConsumption"
    "6edd042e-1478-45f5-9118-d30285253c12:electricityConsumption"
    "b114ecb6-69fd-4d3d-a95a-e6be9a643c17:electricityConsumption"
    "7df3a10e-59b3-4a84-a3a6-c1f83064b8e5:electricityConsumption"
    "69bd639a-c88b-4477-8782-2c4f92a6594e:electricityConsumption"
    "d8d1f13d-2287-4f8f-addf-a9c73780136d:electricityConsumption"
    "7b888fc4-7ae0-422f-827d-e0fb5d9065fe:electricityConsumption"
    "a41a2679-b525-4f42-9958-040b8c2c4a39:electricityConsumption"
    "330003dd-f0f5-4c39-95bb-581c53523aaa:electricityConsumption"
    "4c525817-6454-48f5-90f6-20a3a83c4779:electricityConsumption"
    "e75fb81b-814a-414c-95ea-a5f65e36fea4:electricityConsumption"
    "8b87e15a-6a96-437b-be9f-874cf014d8cd:electricityConsumption"
    "83948fc6-46a7-4c54-b1a4-9f534033af3f:electricityConsumption"
    "faa05896-87f5-472c-b6b2-e0b6d3f7bf72:electricityConsumption"
    "fa02f308-12fb-4bae-ba2f-0f1a0b85ddee:electricityConsumption"
    "d86993f2-a041-40c4-a7b3-aca554e525c3:electricityConsumption"
    "6e10b3b6-dcdd-40f0-8198-55706fb7c726:electricityConsumption"
    "8621de41-3982-467e-b35b-cc118c9850cb:gasAsHeatingFuel"
    "662d7079-0604-4bd6-9ae5-dce0312bb35a:gasAsHeatingFuel"
    "8b47675b-3060-4002-beb7-924f26a886e9:gasAsHeatingFuel"
    "337c342d-abfc-40e4-8279-8344fdab32fe:gasAsHeatingFuel"
    "607ddb92-1fc5-4ec1-a1dd-6fc7c49f585d:gasAsHeatingFuel"
    "405590f2-af41-4a6f-82e4-5829b39b1086:gasAsHeatingFuel"
    "fcaa5e3b-57c5-40a7-a54a-4f0d790da3a2:gasAsHeatingFuel"
    "0478f36c-8751-4c1d-af56-e5d6dc7d380d:gasAsHeatingFuel"
    "745143c7-37d2-4e7f-aa00-a550df34e9aa:gasAsHeatingFuel"
    "fb18d9ff-2f01-432e-af92-1a9961f8a3fb:gasAsHeatingFuel"
    "89d46428-5e47-45cb-b760-f919ab52038a:gasAsHeatingFuel"
    "6ae7adc3-7bdb-41cb-9492-a87a999ea5e0:gasAsHeatingFuel"
    "c3bed4c2-92fd-459a-bccc-5236301c512e:gasAsHeatingFuel"
    "585f4524-883e-4cdb-82ff-f400030653b2:gasAsHeatingFuel"
    "ea0e47e1-b7cf-466f-a526-a31376ea8b06:gasAsHeatingFuel"
    "fc50a348-2159-4a16-bc6c-29e59de80212:gasAsHeatingFuel"
    "ac8adb4e-39ea-4016-8b57-4bfda46ee7d9:gasAsHeatingFuel"
    "8f5c55cc-88ef-4927-b7ca-dec9c21182b6:gasAsHeatingFuel"
    "344aa09b-544e-47b7-852a-5019754b2193:gasAsHeatingFuel"
    "662d7079-0604-4bd6-9ae5-dce0312bb35a:gasConsumption"
    "6ae7adc3-7bdb-41cb-9492-a87a999ea5e0:gasConsumption"})

(defn measurements [input trap]
  (infof "Getting Measurements.")
  (<- [?sensor-id ?tstamp ?value]
      (input ?line)
      (etl/data-line? ?line)
      (non-aggregation? ?line)
      (etl/split-sstable-row ?line :> ?sensor-id-hex ?sensor-data)
      (sensor-has-data? ?sensor-data)
      (etl/unhexify ?sensor-id-hex :> ?sensor-id)
      (contains? interesting-sensors ?sensor-id)
      ;;(verticalize-measurement-rows ?sensor-data :> ?tstamp ?value)
      (breakout-readings ?sensor-data :> ?reading)
      (non-error-reading? ?reading)
      (reading-data ?reading :> ?tstamp ?value)
      (:trap trap)))

