(ns dev.doomsun.data-mapper
  (:require [clojure.alpha.spec :as s]))

(s/def ::sequence-descriptor-schema
  (s/schema {:xf fn?}))

(s/def ::transduce-descriptor-schema
  (s/schema {:xf fn?
             :f fn?
             :init any?}))
(s/def ::into-descriptor-schema
  (s/schema {:xf fn?
             :into coll?}))

(s/def ::mapping-descriptor
  (s/map-of ::destination-descriptor ::value-descriptor))

(s/def ::destination-descriptor
  (s/or :keypath vector?
        :keyset set?
        :key any?))
(s/def ::value-descriptor-schema
  (s/schema {:default any?

             :key any?
             :keypath vector?
             :key-fn fn?
             :keypath-fn fn?
             :value-fn fn?
             :cvalue-fn fn?

             :mapping-descriptor (s/or :fn fn?
                                       :map ::mapping-descriptor)
             :xform fn?
             :cxform fn?
             :sequence ::sequence-descriptor-schema
             :transduce ::transduce-descriptor-schema
             :into ::into-descriptor-schema}))

(s/def ::value-descriptor
  (s/select ::value-descriptor-schema [{:sequence [:xf]
                                        :transduce [:xf]
                                        :into [:coll]}]))


(declare mapper)

(def not-found
  "Not found sentinel, used to distinguish between nil and not found"
  ::not-found)

(defn normalize-mapping-descriptor
  "Normalize a mapping descriptor.

  - Destination keys normalized as keypaths
  - Value descriptors normalized to maps with :keypath, :value-fn, or :cvalue-fn
 "
  [mapping-descriptor]
  (into {}
        (comp
         (mapcat (fn [[dkey desc :as map-entry]]
                   (if (set? dkey)
                     (mapv vector dkey (repeat desc))
                     [map-entry])))
         (map (fn [[dkey desc :as map-entry]]
                (cond
                  (map? desc) map-entry
                  (vector? desc) [dkey {:keypath desc}]
                  :else [dkey {:keypath [desc]}])))
         (map (fn [[dkey desc :as map-entry]]
                (if (:key desc)
                  [dkey (assoc desc :keypath [(:key desc)])]
                  map-entry)))
         (map (fn [[dkey desc :as map-entry]]
                (cond
                  (:key-fn desc) [dkey
                                  (assoc desc
                                         :keypath [((:key-fn desc) (cond-> dkey
                                                                     (coll? dkey) first))])]
                  (:keypath-fn desc) [dkey
                                      (assoc desc
                                        :keypath ((:keypath-fn desc) dkey))]
                  :default map-entry)))
         (map (fn [[dkey desc]]
                [(cond-> dkey (not (vector? dkey)) vector) desc])))
        mapping-descriptor))

(defn apply-value-descriptor
  [{::keys [catch-exceptions default-cxform] :as context} desc input]
  (try
    (let [{:keys [keypath
                  keypaths
                  value-fn
                  cvalue-fn
                  default
                  mapping-descriptor]} desc
          mapping-descriptor (cond-> mapping-descriptor
                                     (fn? mapping-descriptor)
                                     (.invoke context))
          sval (cond
                 keypath (get-in input keypath not-found)
                 keypaths (into {}
                                (map (fn [[k kp]]
                                       [k (get-in input kp not-found)]))
                                keypaths)
                 value-fn (value-fn input)
                 cvalue-fn (cvalue-fn context input))]
      (if (= not-found sval)
        (or default not-found)
        (let [is-single (not (sequential? sval))
              ssval     (cond-> sval is-single vector)
              xformed   (cond
                          (:sequence desc)
                          (let [sequence-desc (cond-> (:sequence desc)
                                                      (fn? (:sequence desc))
                                                      (.invoke context))
                                xf (:xf sequence-desc)]
                            (sequence xf ssval))

                          (:transduce desc)
                          (let [transduce-desc (cond-> (:transduce desc)
                                                       (fn? (:transduce desc))
                                                       (.invoke context))
                                xf (:xf transduce-desc)
                                {:keys [f init]} transduce-desc
                                f (or f conj)
                                init (or init (f))]
                            (transduce xf f init ssval))

                          (:into desc)
                          (let [into-desc (:into desc)
                                coll (:coll into-desc)
                                xf (:xf into-desc)]
                            (if xf
                              (into coll xf ssval)
                              (into coll ssval)))

                          :default
                          (let [{:keys [xform cxform]} desc]
                            (cond-> (transduce
                                     (comp
                                      (cond
                                        xform
                                        (map xform)

                                        cxform
                                        (map (partial cxform context))

                                        mapping-descriptor
                                        (map (partial mapper
                                                      context
                                                      mapping-descriptor))

                                        default-cxform
                                        (map (partial default-cxform context))

                                        :default (map identity)))
                                     conj
                                     []
                                     ssval)
                                    is-single first)))]
          xformed)))
    (catch Exception e
      (if catch-exceptions
        e
        (throw e)))))

(defn mapper
  "
  Map and possibly transform data according to a mapping descriptor.

  #### Args
  - `context`, (optional) data for use in mapping, keys with namespace
    \"dev.doomsun.data-mapper\" are reserved for mapper configuration
  - `mapping-descriptor`, describes the mapping
  - `input`, is the source data; a map of data or a sequence of maps of data

  #### Mapping Descriptors
  A mapping descriptor is a map, where the keys are destinations and the values
  describe the sources using value descriptors.

  A destination key can be:
  - a single destination key
  - a destination keypath (vector)
  - a set of destination keys for which the same value-descriptor is applied

  ##### Value Descriptors
  A value-descriptor can be one of:
  - a source key in the input
  - a source keypath (vector) in the input
  - a map containing some of the following:
    - `:default`, (optional), a default value if no value found

    Required one of:
    - `:key`, a key to look up in the source
    - `:keypath`, a keypath to look up in the source
    - `:keypaths`, multiple named keypaths of the form {<name> <keypath> ...}
    - `:key-fn`, a function of the destination key that returns a key to look up
      in the source
    - `:keypath-fn`, a function of the destination key that returns a keypath to
      look up in the source
    - `:value-fn`, a function of the source data returning a value
    - `:cvalue-fn`, arity-2 function, like :value-fn but the mapping context
    will be passed as the first arg

    If no value is found using :key or :keypath or if :value-fn or :cvalue-fn
    return :dev.doomsun.data-mapper/not-found (and no :default value is
    provided), then the destination key will not be included in the result.

    Optionally one of:
    - `:mapping-descriptor`, recursive step, a mapping descriptor or a function
      of the context that returns a mapping descriptor; applied to each item in
      a sequence
    - `:xform`, a function to transform the looked up or computed value. Will be
      applied to each item in a sequence
    - `:cxform`, an arity-2 function like :xform but the mapping context will be
      passed as the first arg
    - `:sequence`, a map or a function of the mapping context returning a map
      describing a lazy sequence transduction with the
      following keys:
      - `:xf`, a transducer
    - `:transduce`, a map or a function of the mapping context returning a map
      describing an eager transduction with the following keys:
      - `:xf`, a transducer
      - `:f`, (optional) a reducing step function
      - `:init`, (optional) initial value
    - `:into`
      - `:coll`, collection to put values into
      - `:xf`, (optional) a transducer

  Context options:
  - :dev.doomsun.data-mapper/halt-when, a transducer created with
    clojure.core/halt-when applied to the application of value-descriptors.
    - `pred` operates on a tuple of the form [destination-keypath value]
    - `retf`'s return value will be the result of the mapping, otherwise the
      tuple value halted on
  - :dev.doomsun.data-mapper/catch-exceptions, boolean, catch exceptions and
    include as values in result (then you can use your halt-when
    transducer to handle it!)
  - :dev.doomsun.data-mapper/default-cxform, a cxform applied if one is not
  provided in a value-descriptor. A function of context and value"
  ([mapping-descriptor input]
   (mapper {} mapping-descriptor input))
  ([{::keys [halt-when] :as context} mapping-descriptor input]
   (transduce
    (cond->
     (keep (fn [[dkeypath vdesc]]
             (let [v (apply-value-descriptor context vdesc input)]
               (if (= not-found v) nil [dkeypath v]))))
     halt-when (comp halt-when))
    (fn
      ([m [dkeypath v]]
       (assoc-in m dkeypath v))
      ([m] m))
    {}
    (normalize-mapping-descriptor mapping-descriptor))))

(s/fdef mapper
  :args (s/alt :with-context (s/cat :context map?
                                    :mapping-descriptor ::mapping-descriptor
                                    :input any?)
               :no-context (s/cat :mapping-descriptor ::mapping-descriptor
                                  :input any?)))


;; Examples
(comment

 ;; basic
 (mapper {:example/x :a} {:a 5})

 ;; with transform
 (mapper {:x {:key   :a
              :xform inc}}
         {:a 5})

 ;; with context
 (mapper {:val 2}
         {:example/x {:key    :a
                      :cxform (fn [c n] (+ (:val c) n))}}
         {:a 5})


 ;; transform a collection
 (mapper {}
         {:example/x {:key   :a
                      :xform inc}}
         {:a [5 6 7]})

 ;; transduce a collection
 (mapper {}
         {:example/a-inc-sum {:key       :a
                              :transduce {:xf (map inc)
                                          :f  +}}}
         {:a [5 6 7]})


 ;; transduce a collection with context
 (mapper {:inc-amount 5}
         {:example/a-inc-sum {:key       :a
                              :transduce (fn [context]
                                           {:xf (map (partial + (:inc-amount context)))
                                            :f  +})}}
         {:a [5 6 7]})


 ;; transform an infinite collection lazily
 (take 5
       (:example/odd-numbers
        (mapper {}
                {:example/odd-numbers {:key      :numbers
                                       :sequence {:xf (filter odd?)}}}
                {:numbers (iterate inc 0)})))

 ;; do something that might throw an exception, return as value
 (mapper {:dev.doomsun.data-mapper/catch-exceptions true
          :dev.doomsun.data-mapper/halt-when        (halt-when (fn [[_ maybe-exception]]
                                                                 (instance? Exception maybe-exception))
                                                               (fn [r [kp v]]
                                                                 (assoc-in r kp v)))}
         (array-map :x :a
                    :y {:key   :b
                        :xform inc}
                    :z :c)
         {:a "foo"
          :b "baz"
          :c "🤷🏻‍♂️"})

 ;; Use into to get unique values
 (mapper {}
         {:uniques {:key  :data
                    :into {:coll #{}}}}
         {:data [1 1 5 3 3 5 2 1 5]})

 ;; multiple keypaths
 (mapper {}
         {:a {:keypaths {:x [:x] :y [:y]}
              :xform    (fn [{:keys [x y]}] (* x y))}}
         {:x 3 :y 5})

 ;; returning :dev.doomsun.data-mapper/not-found from an xform should remove
 ;; the destination key from the result
 (mapper {}
         {:a {:key :x
              :xform (constantly ::not-found)}}
         {:x 4})

 (mapper {}
         {:a {:key :x
              :xform (fnil identity ::not-found)}}
         {:x nil})

 (mapper {:dev.doomsun.data-mapper/default-cxform (fn [context v] (if (nil? v) ::not-found v))}
         {:a :x
          :b :y
          :c :z}
         {:x nil
          :y nil
          :z 4})

 nil)


