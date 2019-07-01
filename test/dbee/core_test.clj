(ns dbee.core-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.tools.logging.test :refer [logged? with-log]]
            [dbee.core :as dbee]
            [honeysql.core :as sql]))

;; -----------------------------------------------------------------------------
;; fixtures

(defn logs-fixture [f]
  (with-log
    (f)))


(def ^:dynamic *exec-ms* 50.0)
(def ^:dynamic *records* [])
(def ^:dynamic *captured-query* nil)

(defn records-fixture [f]
  (binding [*captured-query* nil]
    (with-redefs [dbee/query-executor (fn [query]
                                        (set! *captured-query* query)
                                        (fn [& _] *records*))
                  dbee/elapsed-ms (fn [& _] *exec-ms*)]
      (f))))


(use-fixtures :each logs-fixture records-fixture)

;; -----------------------------------------------------------------------------
;; expand-query

(deftest expand-query-passes-map
  (is (= {:a :b}
         (dbee/expand-query {:a :b}))))

(deftest expand-query-converts-nil
  (is (= {}
         (dbee/expand-query nil))))

(deftest expand-query-converts-keyword-into-all-selector
  (is (= {:from [:table] :select [:*]}
         (dbee/expand-query :table))))

(deftest expand-query-calls-function
  (is (= :something
         (dbee/expand-query (constantly :something)))))

(deftest expand-query-throws-on-other
  (is (thrown? clojure.lang.ExceptionInfo
               (dbee/expand-query "some string"))))

;; -----------------------------------------------------------------------------
;; by

(deftest by-expands-query
  (is (= {:from [:table] :select [:*]}
         (dbee/by :table {}))))

(deftest by-applies-all-kv-pairs
  (is (= {:from [:table]
          :select [:*]
          :where [:and [:= :id 1] [:= :name "John"]]}
         (dbee/by :table {:id 1 :name "John"}))))

;; -----------------------------------------------------------------------------
;; execute

(def long-running-warn-regex
  #"Query completed in [\d.]+ms, exceeding long-running threshold")

(deftest execute-logs-warning-on-long-running-query
  (testing "with short duration"
    (binding [*exec-ms* 499.9]
      (dbee/execute :conn :query)
      (is (not (logged? 'dbee.core :warn long-running-warn-regex)))))

  (testing "with long duration"
    (binding [*exec-ms* 500.1]
      (dbee/execute :conn :query)
      (is (logged? 'dbee.core :warn long-running-warn-regex)))))

(deftest execute-logs-warning-on-custom-threshold
  (testing "with short duration"
    (binding [*exec-ms* 44.9]
      (dbee/execute :conn :query {:long-running-threshold 45})
      (is (not (logged? 'dbee.core :warn long-running-warn-regex)))))

  (testing "with long duration"
    (binding [*exec-ms* 45.1]
      (dbee/execute :conn :query {:long-running-threshold 45})
      (is (logged? 'dbee.core :warn long-running-warn-regex)))))

(deftest execute-returns-result
  (binding [*records* :some-result]
    (let [{result :result} (dbee/execute :conn :query)]
      (is (= :some-result result)))))

(deftest execute-returns-elapsed-ms
  (binding [*exec-ms* 15.0]
    (let [{elapsed :ms} (dbee/execute :conn :query)]
      (is (= 15.0 elapsed)))))

(deftest execute-returns-raw-sql
  (let [{sql :sql} (dbee/execute :conn {:from [:table] :select [:id]})]
    (is (= ["SELECT id FROM table"]
           sql))))

(deftest execute-returns-query
  (let [input {:from [:table] :select [:id]}
        {output :query} (dbee/execute :conn input)]
    (is (= input output))))

(deftest execute-ensures-query
  (let [{query :query} (dbee/execute :conn :table)]
    (is (= {:from [:table] :select [:*]}
           query))))

(deftest execute-logs-debug-message-upon-completion
  (dbee/execute :conn :query)
  (is (logged? 'dbee.core :debug #"Query completed in [\d.]+ms")))

(deftest execute-rethrows-exception-on-exception
  (let [t (Throwable. "error")
        caught (atom nil)]
    (with-redefs [dbee/query-executor (constantly (fn [& _] (throw t)))]
      (try
        (dbee/execute :conn :query)
        (catch Throwable c
          (reset! caught c))))
    (is (= t @caught))))

(deftest execute-logs-error-on-exception
  (with-redefs [dbee/query-executor (constantly (fn [& _] (throw (Throwable. "error"))))]
    (try (dbee/execute :conn :query) (catch Throwable t))
    (is (logged? 'dbee.core :error [Throwable #"error"] #"Query threw exception"))))

;; -----------------------------------------------------------------------------
;; all

(deftest all-returns-result
  (binding [*records* [{:id 1 :name "John"}
                       {:id 2 :name "Jane"}]]
    (is (= *records* (dbee/all :conn :query)))))

;; -----------------------------------------------------------------------------
;; one

(deftest one-returns-result
  (binding [*records* [{:id 1 :name "John"}]]
    (let [result (dbee/one :conn :query)]
      (is (= {:id 1 :name "John"}
             result)))))

(deftest one-throws-on-multiple-records
  (binding [*records* [{:id 1 :name "John"}
                       {:id 2 :name "Jane"}]]
    (is (thrown? Throwable
                 (dbee/one :conn :query)))))

(deftest one-returns-nil-on-no-records
  (binding [*records* []]
    (let [{result :result} (dbee/one :conn :query)]
      (is (nil? result)))))

;; -----------------------------------------------------------------------------
;; one!

(deftest one!-returns-result
  (binding [*records* [{:id 1 :name "John"}]]
    (let [result (dbee/one! :conn :query)]
      (is (= {:id 1 :name "John"}
             result)))))

(deftest one!-throws-on-multiple-records
  (binding [*records* [{:id 1 :name "John"}
                       {:id 2 :name "Jane"}]]
    (is (thrown? Throwable
                 (dbee/one! :conn :query)))))

(deftest one!-throws-on-no-records
  (binding [*records* []]
    (is (thrown? Throwable
                 (dbee/one! :conn :query)))))

;; -----------------------------------------------------------------------------
;; get

(deftest get-expands-query-with-id
  (binding [*records* [{:id 1 :name "John"}]]
    (dbee/get :conn :table 1)
    (is (= {:select [:*]
            :from [:table]
            :where [:= :id 1]}
           *captured-query*))))

(deftest get-expands-query-with-specified-id
  (binding [*records* [{:id 1 :name "John"}]]
    (dbee/get :conn :table 1 {:primary-key :some-field})
    (is (= {:from [:table]
            :select [:*]
            :where [:= :some-field 1]}
           *captured-query*))))

(deftest get-returns-matching-record
  (binding [*records* [{:id 1 :name "John"}]]
    (is (= {:id 1 :name "John"}
           (dbee/get :conn :query 1)))))

(deftest get-throws-exception-on-multiple-records
  (binding [*records* [{:id 1 :name "John"}
                       {:id 2 :name "Jane"}]]
    (is (thrown? Throwable
                 (dbee/get :conn :query 1)))))

(deftest get-returns-nil-on-no-records
  (binding [*records* []]
    (is (nil? (dbee/get :conn :query 1)))))

;; -----------------------------------------------------------------------------
;; get!

(deftest get!-expands-query-with-id
  (binding [*records* [{:id 1 :name "John"}]]
    (dbee/get! :conn :table 1)
    (is (= {:select [:*]
            :from [:table]
            :where [:= :id 1]}
           *captured-query*))))

(deftest get!-expands-query-with-specified-id
  (binding [*records* [{:id 1 :name "John"}]]
    (dbee/get! :conn :table 1 {:primary-key :some-field})
    (is (= {:from [:table]
            :select [:*]
            :where [:= :some-field 1]}
           *captured-query*))))

(deftest get!-returns-matching-record
  (binding [*records* [{:id 1 :name "John"}]]
    (is (= {:id 1 :name "John"}
           (dbee/get! :conn :query 1)))))

(deftest get!-throws-exception-on-multiple-records
  (binding [*records* [{:id 1 :name "John"}
                       {:id 2 :name "Jane"}]]
    (is (thrown? Throwable
                 (dbee/get! :conn :query 1)))))

(deftest get!-throws-on-no-records
  (binding [*records* []]
    (is (thrown? Throwable
                 (dbee/get! :conn :query 1)))))

;; -----------------------------------------------------------------------------
;; get-by

(deftest get-by-expands-query
  (binding [*records* [{:id 1 :name "John"}]]
    (dbee/get-by :conn :query {:id 1234 :name "Some name"})
    (is (= {:from [:query]
            :select [:*]
            :where [:and [:= :id 1234] [:= :name "Some name"]]}
           *captured-query*))))

(deftest get-by-returns-matching-record
  (binding [*records* [{:id 1 :name "John"}]]
    (is (= {:id 1 :name "John"}
           (dbee/get-by :conn :query {:key :value})))))

(deftest get-by-throws-exception-on-multiple-records
  (binding [*records* [{:id 1 :name "John"}
                       {:id 2 :name "Jane"}]]
    (is (thrown? Throwable
                 (dbee/get-by :conn :query {:key :value})))))

(deftest get-by-returns-nil-on-no-records
  (binding [*records* []]
    (is (nil? (dbee/get-by :conn :query {:key :value})))))

;; -----------------------------------------------------------------------------
;; get-by!

(deftest get-by!-expands-query
  (binding [*records* [{:id 1 :name "John"}]]
    (dbee/get-by! :conn :query {:id 1234 :name "Some name"})
    (is (= {:from [:query]
            :select [:*]
            :where [:and [:= :id 1234] [:= :name "Some name"]]}
           *captured-query*))))

(deftest get-by!-returns-matching-record
  (binding [*records* [{:id 1 :name "John"}]]
    (is (= {:id 1 :name "John"}
           (dbee/get-by! :conn :query {:key :value})))))

(deftest get-by!-throws-exception-on-multiple-records
  (binding [*records* [{:id 1 :name "John"}
                       {:id 2 :name "Jane"}]]
    (is (thrown? Throwable
                 (dbee/get-by! :conn :query {:key :value})))))

(deftest get-by!-throws-on-no-records
  (binding [*records* []]
    (is (thrown? Throwable
                 (dbee/get-by! :conn :query {:key :value})))))

;; -----------------------------------------------------------------------------
;; aggregate

(deftest aggregate-expands-query
  (dbee/aggregate :conn :table :count :id)
  (is (= {:from [:table]
          :select [:%count.id]}
         *captured-query*)))

(deftest aggregate-returns-aggregated-result
  (binding [*records* [{:count 4}]]
    (is (= 4
           (dbee/aggregate :conn :table :count :id)))))

(deftest aggregate-throws-on-unsupported-aggregate-fn
  (is (thrown? Throwable
               (dbee/aggregate :conn :query :UNSUPPORTED :field))))

;; -----------------------------------------------------------------------------
;; delete

(deftest delete-creates-correct-query
  (dbee/delete :conn :table 1)
  (is (= {:delete-from :table
          :where [:= :id 1]}
         *captured-query*)))

(deftest delete-creates-correct-query-using-specified-primary-key
  (dbee/delete :conn :table 1 {:primary-key :field})
  (is (= {:delete-from :table
          :where [:= :field 1]}
         *captured-query*)))

(deftest delete-creates-correct-query-from-record
  (dbee/delete :conn :table {:id 3 :name "Eve"})
  (is (= {:delete-from :table
          :where [:= :id 3]}
         *captured-query*)))

(deftest delete-creates-correct-query-from-record-using-specified-primary-key
  (dbee/delete :conn :table {:some-id 3 :name "Eve"} {:primary-key :some-id})
  (is (= {:delete-from :table
          :where [:= :some-id 3]}
         *captured-query*)))

(deftest delete-throws-when-no-id-can-be-found
  (is (thrown? Throwable
               (dbee/delete :conn :table {:name "Eve"}))))

(deftest delete-returns-affected-record-count
  (binding [*records* [1]]
    (is (= 1
           (dbee/delete :conn :table 1)))))

;; -----------------------------------------------------------------------------
;; delete-all

(deftest delete-all-creates-correct-query
  (dbee/delete-all :conn :table {:where [:and [:= :name "John"] [:< :age 18]]})
  (is (= {:delete-from :table
          :where [:and [:= :name "John"] [:< :age 18]]}
         *captured-query*)))

(deftest delete-all-returns-affected-record-count
  (binding [*records* [1]]
    (is (= 1
           (dbee/delete-all :conn :query {})))))

;; -----------------------------------------------------------------------------
;; exists?

(deftest exists?-creates-correct-query
  (dbee/exists? :conn :table)
  (is (= {:from [:table]
          :select [:*]
          :limit 1}
         *captured-query*)))

(deftest exists?-returns-false-when-no-record-found
  (binding [*records* []]
    (is (false? (dbee/exists? :conn :table)))))

(deftest exists?-returns-true-when-record-found
  (binding [*records* [{:some :record}]]
    (is (true? (dbee/exists? :conn :table)))))

;; -----------------------------------------------------------------------------
;; insert

(deftest insert-creates-correct-query
  (dbee/insert :conn :table {:id 1234 :name "John"})
  (is (= {:insert-into :table
          :columns [:id :name]
          :values [[1234 "John"]]}
         *captured-query*)))

(deftest insert-returns-created-record
  (binding [*records* {:id 1234 :name "John"}]
    (is (= {:id 1234 :name "John"}
           (dbee/insert :conn :table {:name "John"})))))

;; -----------------------------------------------------------------------------
;; insert-all

(deftest insert-all-creates-correct-query
  (dbee/insert-all :conn :table [{:name "John" :username "jdoe"}
                                 {:name "Eve" :username "eve2000"}])
  (is (= {:insert-into :table
          :columns [:name :username]
          :values [["John" "jdoe"] ["Eve" "eve2000"]]}
         *captured-query*)))

(deftest insert-all-uses-all-columns-across-all-records
  (dbee/insert-all :conn :table [{:name "John" :username "jdoe"}
                                 {:name "Eve" :favorite_color "pink"}])
  (is (= {:insert-into :table
          :columns [:name :username :favorite_color]
          :values [["John" "jdoe" nil] ["Eve" nil "pink"]]}
         *captured-query*)))

(deftest insert-all-uses-columns-from-opts
  (dbee/insert-all :conn :table [{:name "John" :username "jdoe"}
                                 {:name "Eve" :favorite_color "pink"}]
                   {:columns [:name :favorite_color]})
  (is (= {:insert-into :table
          :columns [:name :favorite_color]
          :values [["John" nil] ["Eve" "pink"]]}
         *captured-query*)))

(deftest insert-all-returns-affected-count
  (binding [*records* '(2)]
    (is (= 2
           (dbee/insert-all :conn :table [{:name "John" :username "jdoe"}
                                          {:name "Eve" :favorite_color "pink"}])))))

;; -----------------------------------------------------------------------------
;; update

(deftest update-generates-the-correct-query
  (dbee/update :conn :users {:id 1 :name "John" :username "johndoe"})
  (is (= {:update :users
          :where [:= :id 1]
          :set {:name "John"
                :username "johndoe"}}
         *captured-query*)))

(deftest update-accepts-primary-key-as-opt
  (dbee/update :conn :users {:some_field 1 :name "John" :username "johndoe"}
               {:primary-key :some_field})
  (is (= {:update :users
          :where [:= :some_field 1]
          :set {:name "John"
                :username "johndoe"}}
         *captured-query*)))

(deftest update-throws-when-no-primary-key-is-found
  (testing "with default id"
    (is (thrown? Throwable
                 (dbee/update :conn :users {:name "John" :username "johndoe"}))))

  (testing "with specified id"
      (is (thrown? Throwable
                   (dbee/update :conn :users
                                {:id 1 :name "John" :username "johndoe"}
                                {:primary-key :some_field})))))

(deftest update-returns-updated-record
  (binding [*records* {:id 1 :name "John" :username "johndoe"}]
    (is (= {:id 1 :name "John" :username "johndoe"}
           (dbee/update :conn :users {:id 1 :name "John" :username "johndoe"})))))
