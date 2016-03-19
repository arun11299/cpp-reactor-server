Design Description:
-------------------

To develop a multi-threaded producer consumer system.
 
There is a single threaded Producer process that randomly generates messages at configurable interval. Each message consist of 2 or more lines, where the first line is always a single character: A,B or C . This specifies message type. The second line onwards is a set a sentences. The producer sends this message on the fixed host and port, this is where the consumer is listening for messages.
 
Once the consumer detects a message, it needs to process it.
These messages need to be processed in parallel, as per the number of worker threads configured.
Each message needs to be processed by several blocks in sequence. Output of one block is input to the other.
This is what the blocks are supposed to do:
 
              Block1                                 Block2                           Block3
              1.Print the input                 1. Print the input              1. print the input.
              2. Covert each word to PigLatin   2. Calculate the Soundex on     2. Calculate frequency of distinct words.
                                                   each word.


If the message type is A, then Block1, Block2 and Block3 must be executed
If the message type is B, then Block2 and Block3
If message type is C, then Block1 and Block3
