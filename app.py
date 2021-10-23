#!/usr/bin/env python3
# make sure you have installed the following dependency
# conda install -c conda-forge mpi4py

import farm as ssp

class Emitter(ssp.SSP_Emitter):
    def code(self) -> None:
        # sending how many task you need
        for i in range (10):
            print("Emitter:", i)
            self.emmit(ssp.SSP_task(i))
        # advertise the computation has finished with EOS (end of stream)
        self.emmit(ssp.SSP_task(None,True))


class Worker(ssp.SSP_Worker):
    def code(self, task: ssp.SSP_task) -> None:
        print("Worker:", task)
        # always test if it is EOS before performing computation 
        if not task.EOS:
            # you can call a pure/stateless function here
            task.data=task.data+1
        self.emmit(task)
        # cleaning data (good practice)
        del task        

class Collector(ssp.SSP_Collector):
    def code(self, task: ssp.SSP_task) -> None:
        # always test if it is EOS before performing computation 
        if not task.EOS:
            # you can call statefull function here
            print("Collector:", task)
        # cleaning data (good practice)
        del task

def main():
    # creating the Farm topology.
    FARM=ssp.Farm(Emitter(),Worker(),Collector(),False,False)
    # executing the Farm topology
    FARM.run_and_wait()
    # cleaning data (good practice)
    del FARM

main()