# ssp4py - Structured Stream Parallelism for Python

**Author: Dalvan Griebler ([Contact: dalvangriebler@gmail.com](mailto:dalvangriebler@gmail.com))**


This project aims to provide a simpler parallel programming abstraction for Python programs over/using [MPI](https://www.open-mpi.org/). 
It is designed based on the structured parallel programming approach and stream processing applications.
It intends to provide parallel patterns suitable to structure and modeling stream-based computations.

`ssp4py` uses the [`mpi4py`](https://mpi4py.readthedocs.io/) as the backend. 


**_Hopefully it can speed-up your machine-learning tasks of data pre-processing data and training models._**

## To-do List

- Support dynamic/ondemand schedule for the Farm pattern
- Support ordering for the Farm pattern
- Support Pipeline pattern
- Support arbitrary nesting of Pipeline and Farm patterns
- Support parallel Map and MapReduce pattern
- Support arbitrary nesting of Pipeline, Farm, Map, and MapReduce patterns
