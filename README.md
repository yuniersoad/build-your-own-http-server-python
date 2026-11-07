# Http 1.1 server for learning

Based on tutorial [build your own webserver](https://build-your-own.org/webserver/) using python instead of original node.js

# Testing

To test several features (echo req body, HEAD, chunked encoding):

- ```(cat ./request1.1.txt; sleep 1;) | nc localhost 1234```


To test response compression:

- ```(cat ./request1.1-compressed.txt; sleep 1;) | nc localhost 1234``` you should see the actual compressed response in the console

- 
