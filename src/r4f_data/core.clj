(ns r4f-data.core
  (:use cascalog.api
        cascalog.checkpoint
        [cascalog.more-taps :only (hfs-delimited)]
        [r4f-data devices measurements retrofit-data]
        [clojure.tools.logging :only (infof errorf)])
  (:gen-class))

;; hadoop jar r4f-data.jar <m7-input> <devices-input> <projects-input> <checkpoint-dir> <output-dir> <trap-dir>
(defn -main [measurements-in devices-in projects-in checkpoint output trap-root & args]
  (workflow
   [checkpoint]
   m7 ([:tmp-dirs [m7-stage]]
         (with-job-conf
           {"mapred.tasktracker.map.tasks.maximum" 1
            "mapred.task.timeout" 1200000
            "mapred.job.shuffle.input.buffer.percent" 0.5
            "mapred.child.java.opts" "-Xmx3072m"
            "mapred.reduce.slowstart.completed.maps" 0.60
            "mapred.reduce.tasks" 1}
           (?- "m7"
               (hfs-delimited m7-stage)
               (measurements (hfs-textline measurements-in)
                             (hfs-delimited (str trap-root "/m7"))))))
   d6 ([:tmp-dirs [d6-stage] :deps :last]
         (?- "d6"
             (hfs-delimited d6-stage)
             (devices (hfs-textline devices-in)
                      (hfs-delimited (str trap-root "/d7")))))
   r8 ([:tmp-dirs [r8-stage] :deps [m7 d6]]
         (with-job-conf
           {"mapred.reduce.tasks" 12
            "mapred.reduce.slowstart.completed.maps" 0.60}
           (?- "r8"
               (hfs-delimited r8-stage)
               (retrofit-data (hfs-delimited m7-stage)
                              (hfs-delimited d6-stage)
                              (hfs-delimited projects-in)
                              (hfs-delimited (str trap-root "/r8"))))))
   good ([:deps :all]
           (with-job-conf
             {"mapred.reduce.tasks" 12
              "mapred.reduce.slowstart.completed.maps" 0.60}
             (?- "gooddata"
                 (hfs-delimited
                  output
                  :sinkmode :replace
                  :sink-template "%s/%s" :templatefields ["?tsb-id" "?safe-sensor-id"]
                  :outfields ["?tsb-id" "?entity-id" "?sensor-id" "?tstamp" "?value" "?units" "?type" "?period"])
                 (good-retrofit-data (hfs-delimited r8-stage)
                                     (hfs-delimited (str trap-root "good"))))))))
