(ns am.ik.cloudure.core
  (:import (org.apache.hadoop.fs Path)
           (org.apache.hadoop.io IntWritable LongWritable Text)
           (org.apache.hadoop.mapreduce Job Mapper Mapper$Context Reducer Reducer$Context)
           (org.apache.hadoop.mapreduce.lib.input FileInputFormat)
           (org.apache.hadoop.mapreduce.lib.output FileOutputFormat))
  (:use am.ik.cloudure.conv))

(defn mapper-conv-write [key value #^Mapper$Context context]
  (.write context (conv key) (conv value)))

(defn reducer-conv-write [key value #^Reducer$Context context]
  (.write context (conv key) (conv value)))

(defn- escape-name [name]
  "escape '-' -> '_'"
  (.replace name \- \_))

(defn- adjust-args [args]
  "push _ until count of args is 3.
   if cout of args is over 3, then return first 3 elements.
  "
  (let [len 3]
    (if (> (count args) len) 
      (subvec args 0 len)
      (loop [args args]
        (if (= (count args) len) args
            (recur (conj args '_)))))))
  
(defmacro defmapreduce [name & options]
  "define Mapper and Reducer"
  (let [args (apply hash-map options)
        mapper (:mapper args)
        reducer (:reducer args)
        mapper-fn? (symbol? mapper)
        reducer-fn? (symbol? reducer)
        class-name (escape-name (str *ns*))
        mapper-name  (escape-name (str *ns* "." name ".mapper"))
        mapper-prefix (str name "-mapper-")
        mapper-args (or mapper-fn? (into ['this] (adjust-args (first mapper))))
        mapper-body (or mapper-fn? (rest mapper))
        reducer-name (escape-name (str *ns* "." name ".reducer"))
        reducer-prefix (str name "-reducer-")
        reducer-args (or reducer-fn? (into ['this] (adjust-args (first reducer))))
        reducer-body (or reducer-fn? (rest reducer))
        get-job (str "get-" name "-job")]
    `(do
       ;; define Mapper
       (gen-class
        :name ~(symbol mapper-name)
        :extends org.apache.hadoop.mapreduce.Mapper
        :prefix ~mapper-prefix)
       (defn ~(symbol (str mapper-prefix "map")) 
         ~@(if mapper-fn? 
             ;; invoke delegated mapper function
             `([_ key# value# context#] (~mapper key# value# context#))
             ;; define mapper method
             `(~mapper-args ~@(if (not (empty? mapper-body)) 
                                (let [lst (last mapper-body)]
                                  (if (vector? lst) 
                                    ;; auto convert
                                    (conj (vec (butlast mapper-body)) 
                                          `(mapper-conv-write ~@lst ~(last mapper-args)))
                                    ;; normal definition
                                    mapper-body))))))
       ;; define Reducer
       (gen-class
        :name ~(symbol reducer-name)
        :extends org.apache.hadoop.mapreduce.Reducer
        :prefix ~reducer-prefix)
       (defn ~(symbol (str reducer-prefix "reduce")) 
         ~@(if reducer-fn? 
             ;; invoke delegated reducer function
             `([_ key# values# context#] (~reducer key# values# context#))
             ;; define reducer method
             `(~reducer-args ~@(if (not (empty? reducer-body))                                  
                                 (let [lst (last reducer-body)]
                                   (if (vector? lst) 
                                     ;; auto convert
                                     (conj (vec (butlast reducer-body)) 
                                           `(reducer-conv-write ~@lst ~(last reducer-args)))
                                     ;; normal definition
                                     reducer-body))))))
       ;; define getter of Job
       (defn ~(symbol get-job)
         ([] (~(symbol get-job) true))
         ([~'set-jar?]
            (let [~'job (Job.)]
              (if ~'set-jar? (.setJarByClass ~'job (Class/forName ~class-name)))
              (doto ~'job
                (.setMapperClass (Class/forName ~mapper-name))
                (.setReducerClass (Class/forName ~reducer-name)))))
         {:tag Class}))))

(comment 
  ;; how to define MapReduce like this
  (defn hello-map [key value context]
    (.write context (Text. key) (IntWritable. (Integet/parseInt value))))
  (defn hello-reduce [key values context]
    (.write context (IntWritable. (reduce + (map #(.get %) values)))))
  (defmapreduce hello
    :mapper hello-map
    :reducer hello-reduce)

  ;; or
  (defmapreduce hello
    :mapper ([key value context] (.write context (Text. key) (IntWritable. (Integet/parseInt value))))
    :reducer ([key values context] (.write context key (IntWritable. (reduce + (map #(.get %) values))))))
  ;; This definition generate hello.mapper class which extends org.apache.hadoop.mapreduce.Mapper and invokes hello-map in Mapper#map method
  ;; and hello.reducer class which extend org.apache.hadoop.mapreduce.Reducer and invokes hello-reduce in Reducer#reduce method.  
  ;; You can get Job by call (get-hello-job) which is set Mapper class and Reducer class defined above
  )