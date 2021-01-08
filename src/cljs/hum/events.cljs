(ns hum.events
  (:require
   [ajax.core :as ajax]
   [re-frame.core :as rf]
   [hum.db :as db]
   [day8.re-frame.tracing :refer-macros [fn-traced]]))

(defn request [params]
  (merge {:format (ajax/json-request-format)
          :response-format (ajax/json-response-format
                            (select-keys params [:keywords?]))}
         params))

(rf/reg-event-db
 ::initialize-db
 (fn-traced [_ _]
            db/default-db))

(rf/reg-event-db
 ::set-active-panel
 (fn-traced [db [_ active-panel]]
            (assoc db :active-panel active-panel)))

(rf/reg-event-fx
 ::fetch-data
 (fn [{db :db} [_]]
   {:http-xhrio (request
                 {:method :get
                  :keywords? true
                  :uri "http://localhost:5000"
                  :on-success [::fetch-data.success]
                  :on-failure [::fetch-data.failure]})}))

(rf/reg-event-db
 ::fetch-data.success
 (fn [db [_ resp]]
   (assoc-in db [:data] resp)))

(comment
  (rf/dispatch [::fetch-data]))
