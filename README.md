# Estuary
Dictionary designed for read-mostly scene.

![](images/estuary-read.png)
It can provide 200M read QPS on single machine.
![](images/estuary-write.png)
And more than 2M write QPS without persistence.

### Key Features
* lock-free read
* high read performance
* support key and value with variable length
* have a very low failure rate in theory
* aceptable space overhead (ablout 21 bytes per item + 10% data size)
* work on 64bit CPU with little-endian memory order


## The Lucky Version

![](images/lucky-read.png)
It can provide up to half a billion read QPS per machine.
![](images/lucky-write.png)
And more than one million write QPS without persistence.

### Key Features
* lock-free read
* very high read performance
* key and value should have fixed size
* actually work, but not be theoretically safe (we are usually lucky enough)
* resonable space overhead (ablout 10 bytes per item)
* work on 64bit CPU with little-endian memory order


## Other Read-only Solutions
* [low space overhead](https://github.com/PeterRK/fastCHD)
* [high read performance](https://github.com/PeterRK/SSHT)

---
[【Chinese】](README-CN.md) [【English】](README.md)
