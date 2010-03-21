(ns am.ik.cloudure.conv
  (:import (org.apache.hadoop.io BooleanWritable ByteWritable IntWritable LongWritable FloatWritable DoubleWritable 
                                 NullWritable Text BytesWritable MD5Hash ObjectWritable GenericWritable
                                 ArrayWritable TwoDArrayWritable MapWritable SortedMapWritable)
           (org.apache.hadoop.mapreduce Mapper$Context Reducer$Context)))

(defmulti conv class)

(defmethod conv :default [x]
  x)

(defmacro defconv [conv-map]
  `(do
     ~@(for [c conv-map]
         `(defmethod conv ~(first c) [x#]
            (new ~(second c) x#)))))

(defn conv-write [key value context]
  (.write context (conv key) (conv value)))

(defconv {Boolean BooleanWritable,
          Integer IntWritable,
          Long LongWritable,
          Float FloatWritable,
          Double DoubleWritable,
          Byte ByteWritable,
          String Text,
          Object ObjectWritable})