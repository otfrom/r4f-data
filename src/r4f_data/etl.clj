(ns r4f-data.etl
  (:require [cheshire.core :as json]
            [clojure.string :as string])
  (:use clojure.tools.logging))

;; hexify and uhexify thanks to Neale Swinnerton @sw1nn
;; London Clojurian and all round good guy
;; http://stackoverflow.com/questions/10062967/clojures-equivalent-to-pythons-encodehex-and-decodehex
(defn hexify [^String s]
  (apply str (map #(format "%02x" %) (.getBytes s "UTF-8"))))

(defn unhexify [^String s]
  (let [^bytes bytes (into-array Byte/TYPE
                                 (map (fn [[x y]]
                                        (unchecked-byte (Integer/parseInt (str x y) 16)))
                                      (partition 2 s)))]
    (String. bytes "UTF-8")))

(defn split-sstable-row [^String row]
  ;; find the rowkey - data separator
  ;; trim the extra quotes of the row key
  (infof "Splitting sstable row of length %s" (.length row))
  (let [split-idx (.indexOf row "\": ")
        hexed-key (string/replace (.substring row 0 split-idx) "\"" "")
        data (string/trim (.substring row (+ 3 split-idx)))]
    [hexed-key data]))

(defn data-line? [^String row]
  (and (not= -1 (.indexOf row ":"))
       (.startsWith row "\"")
       (.endsWith row ",")))

