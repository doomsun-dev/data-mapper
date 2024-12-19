# DataMapper

A no-frills, light on dependencies library for mapping and transforming data 
using a simple data-driven DSL.

Some design goals include:
- reduce ambiguity and reliance on conventions
- as much ambivilence as possible regarding the shape of input and output data
  - e.g., keys and values can be anything and therefore mapping descriptors 
  must be explicit when referring to keys vs key paths.
- require the explicit specification of destination keys/keypaths

Due to these goals, mapping descriptors have a lot of options.

## Usage

There is basically one function, `dev.doomsun.data-mapper/mapper`, and here is 
how you use it.

#### Description
Map and transform data according to a mapping descriptor.

Map and possibly transform data according to a mapping descriptor.

#### Args
- `context`, (optional) data for use in mapping, keys with namespace
  "dev.doomsun.data-mapper" are reserved for mapper configuration
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
        - `:cxf`, (optional) a function that is passed the mapping context and 
        returns transducer 

Context options (can also be included in the mapping descriptor, context keys
take precedence):
- :dev.doomsun.data-mapper/halt-when, a transducer created with
  clojure.core/halt-when applied to the application of value-descriptors.
    - `pred` operates on a tuple of the form [destination-keypath value]
    - `retf`'s return value will be the result of the mapping, otherwise the
      tuple value halted on
- :dev.doomsun.data-mapper/catch-exceptions, boolean, catch exceptions and
  include as values in result (then you can use your halt-when
  transducer to handle it!)
- :dev.doomsun.data-mapper/default-cxform, a cxform applied if one is not
  provided in a value-descriptor. A function of context and value
- :dev.doomsun.data-mapper/remove-nil-values, boolean, remove nil values from
  result
