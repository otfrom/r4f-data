(ns r4f-data.devices
  (:require [r4f-data.etl :as etl]
            [cheshire.core :as json]
            [clojure.string :as string])
  (:use cascalog.api
        clojure.tools.logging))

(defn sensor-id [device-id device-type]
  (str device-id ":" device-type))

(defn cols->map [x]
  (into {} (map (fn [[colname data _]] [(keyword colname) data]) x)))

(defn sensors [{:keys [deviceId entityId readings]}]
  (format "Getting sensors from device: %s" deviceId)
  (map (fn [{:strs [type unit period]}]
       [(sensor-id deviceId type)
        (str deviceId)
        (str entityId)
        (str type)
        (str unit)
        (str period)])
     (json/parse-string readings)))

(defmapcatop sensor-records [device]
  (infof "Creating sensor records from: %s" device)
  (-> (json/parse-string device)
      cols->map
      sensors))

(defn devices [devices-in trap]
  (infof "Querying devices from %s" devices-in)
  (<- [?sensor-id ?device-id ?entity-id ?type ?units ?period]
      (devices-in ?line)
      (etl/data-line? ?line)
      (etl/split-sstable-row ?line :> ?rowkey-hex ?device)
      (sensor-records ?device :> ?sensor-id ?device-id ?entity-id ?type ?units ?period)
      (:trap trap)))

