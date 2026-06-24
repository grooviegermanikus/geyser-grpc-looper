
Why you want that?
* useful if you only need commitment level CONFIRMED; not useful if you need PROCESSED
* messages will reach clients earlier because they will not wait in the buffer on validator side
* messages will not be sent in a single burst to client
* pressure on memory system on the validator node will be reduced - but will be higher on client side

How it works?
* problematic flow inside the yellowstone geyser plugin:
  * plugin buffers a lot of messages from the validated when block gets processed
  * plugin releases all data when the slot confirmed notification is seen
* looper pulls the filtered data from yellowstone-geyser plugin with commitment level PROCESSED only which gives a contiguous stream of data
* looper implements the same buffer-emit-logic as the yellowstone-geyser plugin but on client side

Versioning:
* main branch points to the latest supported yellowstone version
* older yellowstone versions are maintained on branches like update-yellowstone-grpc-11