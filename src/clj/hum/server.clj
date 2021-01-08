(ns hum.server
  (:require [hum.handler :refer [handler]]
            [config.core :refer [env]]
            [clj-time.format :as f]
            [clj-time.core :as time]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [semantic-csv.core :as sc]
            [ring.adapter.jetty :refer [run-jetty]])
  (:gen-class))

(defn -main [& _args]
  (let [port (or (env :port) 3000)]
    (run-jetty handler {:port port :join? false})))

(comment (+ 1 1)
         (with-open [reader (io/reader "csv_challenge.csv")]
           (->> (csv/read-csv reader)
                first)))

(defn prep-data
  "Lazily evaluates rows and converts rows containing into maps of
  headers->column-values"
  [reader]
  (->> (csv/read-csv reader)
       (sc/mappify {:transform-header sc/->idiomatic-keyword})))

(def csv-data
  "Forces evaluation of our lazy seq of csv so we can re-use the data without
  needing to pay the overhead of opening, reading and closing file for eqch
  question"
  (with-open [reader (io/reader "csv_challenge.csv")]
    (doall (prep-data reader))))

(defn sum-trx
  "Extracts :amount from each trx, sums the values, then formats into decimal"
  [rows]
  (->> rows
       (map :amount)
       (apply +)
       (format "%.2f")))

(defn question-1
  "How much was spent on the Wells Fargo debit card?

  Solution: 10153596.52
  "
  []
  (->> csv-data
       (sc/cast-with {:amount sc/->float})
       (filter #(= "Wells Fargo" (:financial-institution %)))
       (sum-trx)))

(defn question-2
  "How many unique vendors did the company transact with?

  Solution: 16
  "
  []
  (->> csv-data
       (sc/cast-with {:amount sc/->float})
       (map :vendor)
       distinct
       count))

(defn question-3
  "The company is worried too much money is being spent on perks for employees.
  How much is being spent on items tagged either \"personal\" or \"food\"?

  Solution: 2624266.34
  "
  []
  (->> csv-data
       (sc/cast-with {:amount sc/->float
                      :tags (fn [trx-tags]
                              (-> (str/split trx-tags #" ")
                                  set))})
         ;; ["personal" "food"]
       (filter #(some #{"personal" "food"} (:tags %)))
       (sum-trx)))

(defn within-date-range?
  "Assuming inclusive on the begin and exclusive of the end"
  [timestamp begin-ts end-ts]
  (and (or
        (time/before? begin-ts timestamp)
        (time/equal? begin-ts timestamp))
       (time/after? end-ts timestamp)))

(defn within-hour-range?
  "Assuming inclusive on the begin and exclusive of the end"
  [timestamp begin-hour end-hour]
  (let [timestamp-hour (time/hour timestamp)]
    (and (<= begin-hour timestamp-hour)
         (>= end-hour timestamp-hour))))

(defn in-month?
  [trx month]
  (= month (time/month trx)))

(def dt-formatter (f/formatters :date-time-no-ms))

(defn question-4
  "The company had a party in London the weekend of January 23-25, 2012. How
  much did they spend over that period? Tip: The time zone during this period is
  equivalent to UTC.

  Assuming Midnight UTC inclusive on Jan 23 and Midnight UTC exclusive on Jan 26

  Solution: 547975.25
  "
  []
  (let [begin (f/parse dt-formatter "2012-01-23T00:00:00Z")
        end  (f/parse dt-formatter "2012-01-26T00:00:00Z")]
    (->> csv-data
         (sc/cast-with {:amount sc/->float
                        :date (fn [trx-date]
                                (f/parse dt-formatter trx-date))})
         (filter #(-> % :date (within-date-range? begin end)))
         (sum-trx))))

(defn question-5
  "On how many evenings in December did the company buy from at least two
  distinct bars? Only count purchases made in December between 6:00 PM to 12:00
  AM Pacific Time. The names of the bars are: \"RICKHOUSE\", \"P.C.H.\",
  \"BLOODHOUND\", \"IRISH BANK\".

  Assumptions: Daylight Savings time not in effect, so the offset is 8 hours

  convert to Pacific time
  check that month is December
  check for specific vendors

  Solution: 4373.89
  "
  []
  (->> csv-data
       (sc/cast-with {:amount sc/->float
                      :date (fn [trx-date]
                              (-> (f/parse dt-formatter trx-date)
                                  (time/to-time-zone (time/time-zone-for-offset -8))))})
       (filter #(-> % :date (in-month? 12)))
       (filter #(-> % :date (within-hour-range? 18 23)))
       (filter #(#{"RICKHOUSE" "P.C.H." "BLOODHOUND" "IRISH BANK"} (:vendor %)))
       ;;  group-by :date
       (sum-trx)))

;;  taken from SO
(defn average [coll]
  (/ (reduce + coll) (count coll)))

(defn trxs->std-dev
  [trxs mean]
  (-> (map (fn [trx]
             (-> trx
                 :amount
                 (- mean)
                 (#(* % %))))
           trxs)
      average
      Math/sqrt))

(defn suspicious-trx?
  [trx mean std-dev]
  (> (:amount trx) (+ mean (* 1.75 std-dev))))

(defn question-6
  "Search the data for \"suspicious spends\" for each vendor. Suspicious spends
  are defined as more than 1.75 standard deviations above the mean of purchases
  for a given vendor. Print the vendor names and transaction IDs.

  - get trx by vendor
  - calculate mean by vendor
  - calculate std deviation by vendor
  - map over trx per vendor and compare against std deviation
  - return vendor->[suspicious-trx-ids]
  "
  []
  ;; {STARBUCKS [list-of-trxs]
  ;; ...}
  (let [trx-by-vendor (->> csv-data
                           (sc/cast-with {:amount sc/->float})
                           (group-by :vendor))
          ;; {STARBUCKS mean-1}
        mean-trx-by-vendor (into {}
                                 (map (fn [[vendor trxs]]
                                        [vendor (average (map :amount trxs))])
                                      trx-by-vendor))
        std-deviation-by-vendor (into {}
                                      (map (fn [[vendor trxs]]
                                             [vendor (trxs->std-dev trxs (get mean-trx-by-vendor vendor))])
                                           trx-by-vendor))]
      ;; {STARBUCKS [list-of-suspicious-trx-ids]}}
    (map (fn [[vendor trxs]]
           [vendor
            (keep (fn [trx]
                    (when (->> (get std-deviation-by-vendor vendor)
                               (suspicious-trx? trx (get mean-trx-by-vendor vendor)))
                      (:transaction-id trx)))
                  trxs)])
         trx-by-vendor)))

(comment
  (question-1)
  (question-2)
  (question-3)
  (question-4)
  (question-5)
  (question-6))
