(ns max-temperature
  (:gen-class)
  (:import (org.apache.hadoop.fs Path)
           (org.apache.hadoop.io IntWritable LongWritable Text)
           (org.apache.hadoop.mapreduce Job Mapper Mapper$Context Reducer Reducer$Context)
           (org.apache.hadoop.mapreduce.lib.input FileInputFormat)
           (org.apache.hadoop.mapreduce.lib.output FileOutputFormat)
           )
  (:use org.clojars.making.cloudure.core)
  )

(defmapreduce temperature 
  :mapper 
  ([key value context]
     (let [line (str value)
           year (.substring line 15 19)
           quality (.substring line 92 93)
           air-temperature (Integer/valueOf 
                            (.substring line 
                                        (if (= (.charAt line 87) \+) 88 87)
                                        92))]
       (if (and (not (= air-temperature 9999))
                (.matches quality "[01459]"))
         (.write context (Text. year) (IntWritable. air-temperature)))
       ))
  :reducer
  ([key values context]
     (.write context key (IntWritable. (reduce max (map #(.get %) values))))
     ))

(defn -main [& args]
  (when (not (= (count args) 2))
    (.println System/err "args error!")
    (System/exit -1)
    )
  (let [job (get-temperature-job)] ; this job is already set mapper and reducer
    (FileInputFormat/addInputPath job (Path. (nth args 0)))    
    (FileOutputFormat/setOutputPath job (Path. (nth args 1)))
    (doto job      
      (.setOutputKeyClass Text)
      (.setOutputValueClass IntWritable)
      )
    (System/exit (if (.waitForCompletion job true) 0 1))    
    )
  )
