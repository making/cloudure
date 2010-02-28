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

(defmacro defmapreduce [name & options]
  "define Mapper and Reducer"
  (let [args (apply hash-map options)
        mapper (:mapper args)
        reducer (:reducer args)
        mapper-fn? (symbol? mapper)
        reducer-fn? (symbol? reducer)
        class-name (escape-name (str name))
        mapper-name  (str *ns* "." class-name ".mapper")
        mapper-prefix (str name "-mapper-")
        mapper-args (or mapper-fn? (into ['this] (first mapper)))
        mapper-body (or mapper-fn? (rest mapper))
        reducer-name (str *ns* "." class-name ".reducer")
        reducer-prefix (str name "-reducer-")
        reducer-args (or reducer-fn? (into ['this] (first reducer)))
        reducer-body (or reducer-fn? (rest reducer))
        get-job (str "get-" name "-job")
        ]
    `(do
       (gen-class
        :name ~(symbol mapper-name)
        :extends org.apache.hadoop.mapreduce.Mapper
        :prefix ~mapper-prefix
        )       
       (defn ~(symbol (str mapper-prefix "map")) 
         ~@(if mapper-fn? (let [this (gensym) key (gensym) value (gensym) context (gensym)]
                            `([~this ~key ~value ~context] (~mapper ~this ~key ~value ~context)))
               `(~mapper-args ~@(if (not (empty? mapper-body)) mapper-body))
               )
         )
       (gen-class
        :name ~(symbol reducer-name)
        :extends org.apache.hadoop.mapreduce.Reducer
        :prefix ~reducer-prefix
        )
       (defn ~(symbol (str reducer-prefix "reduce")) 
         `(if reducer-fn? (let [this (gensym) key (gensym) values (gensym) context (gensym)]
                            `([~this ~key ~values ~context] (~reducer ~this ~key ~values ~context))))
         `(~reducer-args ~@(if (not (empty? reducer-body)) reducer-body)))
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

(comment 
  ;; how to define MapReduce like this

  (defn hello-map [key value context]
    (.write context (Text. key) (IntWritable. (Integet/parseInt value)))
    )
  (defn hello-reduce [key values context]
    (.write context (IntWritable. (reduce + (map #(.get %) values))))
    )
  (defmapreduce hello
    :mapper hello-map
    :reducer hello-reduce
    )

  ;; or
  (defmapreduce hello
    :mapper ([key value context] (.write context (Text. key) (IntWritable. (Integet/parseInt value))))
    :reducer ([key values context] (.write context (IntWritable. (reduce + (map #(.get %) values)))))
    )
  ;; This definition generate hello.mapper class which extends org.apache.hadoop.mapreduce.Mapper and invokes hello-map in Mapper#map method
  ;; and hello.reducer class which extend org.apache.hadoop.mapreduce.Reducer and invokes hello-reduce in Reducer#reduce method.  
  ;; You can get Job by call (get-hello-job) which is set Mapper class and Reducer class defined above
  )