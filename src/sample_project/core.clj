(ns sample-project.core
  (:gen-class)
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as string]))


(defn write-to-file [path row]
  (if (seq? row)
    (map (partial write-to-file path) row)
    (let [ordered (into (sorted-map) row)]
      (spit path (str (json/generate-string ordered) "\n") :append true))))


(defn is-vector? [data]
  (if (vector? (val data))
    (key data)
    nil))


(defn cast-type [value]
  (if (vector? value)
    value
    (str value)))


(defn rename-key [keyname]
  (cond
    (= keyname "_id_$oid")
    (-> keyname
        (string/replace "_id_" "id_")
        (string/replace #"\$" ""))
    (string/includes? keyname "_$date")
    (string/replace keyname #"_\$date" "")
    :else
    keyname))


(defn sign [prefix]
  (if (nil? prefix)
    nil
    (str prefix "_")))


(defn unnest-data [prefix data]
  (if (map? (val data))
    (mapcat (partial unnest-data (str (sign prefix) (key data))) (val data))
    (if (nil? (val data))
      nil
      {(rename-key (str (sign prefix) (key data))) (cast-type (val data))})))


(defn process-record [record]
  (let [unnested (into {} (mapcat (partial unnest-data nil) record))
        vectkeys (into [] (remove nil? (map is-vector? unnested)))]
    (if (empty? vectkeys)
      unnested
      (let [flatonly (apply dissoc unnested vectkeys)
            vectonly (apply dissoc unnested (keys flatonly))
            exploded (for [x vectkeys] (for [y (vectonly x)] (merge flatonly {x y})))]
        (map process-record (apply concat exploded))))))


(defn call-processing [element]
  (process-record (json/parse-string element)))


(defn read-from-file [file]
  (with-open [rdr (io/reader file)]
    (doall (line-seq rdr))))


(defn -main
  "test"
  []
  (let [dataset (read-from-file "input/source.json")]
    (map (partial write-to-file "output/result.json") (map call-processing dataset))))
