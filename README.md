BTCDB
=====

Open source on-disk index &amp; store based very light weigth library desgned to be used when dealing with bitcoin block chain

The two main modules are SetAByte and MapAByteLong  which corresponds to the standard  Set<byte[]> and Map<byte[],long>  - the implementations store the data on the disk and can be distributed in multipleof files.  The expected usage is to use a SHA256 for the key in byte[] format and either check for its existence in the Set or find its position in a flat file using the Map. The primitives also supply balancing functionality if required - although not really needed when key is a SHA256.  The remove method can be used only for the last inserted element - which is enough when the library is used for implementing blockchain technology.  The implementations behave well with 100's of millions of records and are designed for this type of numbers. It also uses in memory cache for the first elements of the tree which improves the performance enormously - we are talking about microseconds access to a element on descent hardware. Whne using in real scenarion don't forge that the a call to close/fliush is necessery in a shutdown hook - file recovery also supplied as part of the lib as it is mandatory in a real world scenario.  

