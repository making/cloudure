(ns am.ik.cloudure.core
  (:import (org.apache.hadoop.fs Path)
           (org.apache.hadoop.io IntWritable LongWritable Text)
           (org.apache.hadoop.mapreduce Job Mapper Mapper$Context Reducer Reducer$Context)
           (org.apache.hadoop.mapreduce.lib.input FileInputFormat)
           (org.apache.hadoop.mapreduce.lib.output FileOutputFormat)
           ) 
  )

(defn- escape-name [name]
  (.replace name \- \_)
  )

(defmacro defmapreduce [name {:keys [mapper reducer]} & body]
  "define Mapper and Reducer"
  (let [class-name (escape-name (str name))
        mapper-name  (str *ns* "." class-name ".mapper")
        mapper-prefix (str name "-mapper")
        mapper-args (into ['this] (first mapper))
        mapper-body (rest mapper)
        reducer-name (str *ns* "." class-name ".reducer")
        reducer-prefix (str name "-reducer")
        reducer-args (into ['this] (first reducer))
        reducer-body (rest reducer)
        get-job (str "get-" name "-job")        
        ]
    `(do
       (gen-class
        :name ~mapper-name
        :extends org.apache.hadoop.mapreduce.Mapper
        :prefix ~mapper-prefix
        )
       (defn ~(symbol (str mapper-prefix "-map")) 
         ~mapper-args
         ~@(if (not (empty? mapper-body)) mapper-body)
         )
       (gen-class
        :name ~reducer-name
        :extends org.apache.hadoop.mapreduce.Reducer
        :prefix ~reducer-prefix
        )
       (defn ~(symbol (str reducer-prefix "-reduce")) 
         ~reducer-args
         ~@(if (not (empty? reducer-body)) reducer-body)
         )
       (defn ~(symbol get-job)
         ([] (~(symbol get-job) true))
         ([~'set-jar?]
            (let [~'job (Job.)]
              (if ~'set-jar? (.setJarByClass ~'job (Class/forName ~class-name)))
              (doto ~'job
                (.setMapperClass (Class/forName ~mapper-name))
                (.setReducerClass (Class/forName ~reducer-name))
                ))
            )
         {:tag Class}
         )
       )))
