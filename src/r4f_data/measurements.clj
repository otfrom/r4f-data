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

(defn measurements [input trap]
  (infof "Getting Measurements.")
  (<- [?sensor-id ?tstamp ?value]
      (input ?line)
      (etl/data-line? ?line)
      (non-aggregation? ?line)
      (etl/split-sstable-row ?line :> ?sensor-id-hex ?sensor-data)
      (sensor-has-data? ?sensor-data)
      (etl/unhexify ?sensor-id-hex :> ?sensor-id)
      (breakout-readings ?sensor-data :> ?reading)
      (non-error-reading? ?reading)
      (reading-data ?reading :> ?tstamp ?value)
      (:trap trap)))

