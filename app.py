"""This is a demonstrative example for using the Farm pattern abstraction.
    The basics are:
        - Extend the SSP_Emitter (producer), SSP_Worker (consumer and producer), and SSP_Collector (consumer) abstract classes
        - Implement the "code" method which is the computation that will be performed. Note that it is different depending on the abstract class (Farm entity).
        - Pay attention that Emitter emmits a task to Worker that emmits to Collector.
        - The tasks are consumed via "code" method parameter, which is always SSP_task type. 
        - SP_task is a generic data type to be transfered data in the Farm topology. You can stored and access information via "data" attribute. Ex: task.data
        - Emitter has to advertise the end of the stream, which is done via "SSP_task(None,True)" message.
        - Inside the "code" of Worker and Collector classes, make sure to test if the message is an EOS to avoid errors since data is empty. Ex: if not task.EOS: computation()
        - Create the Farm topology with the "Farm" class, which receives as argument:
            1- Emmiter object that send and schedule taks to Worker replicas.
            2- Worker object that is replicated as many as process are left in mpiexec -np . For instance, when "-np 4", there are two Worker replicas.
            3- Collector object that receive the data processed by the Worker replicas.
            4- Boolean value to enable or disable ondmand/dynamic schedule, by default is False that means disable.
            5- Boolean value to enable or disable ordering, by default is False that means disable. When enable, it preserves the order of the task produced by Emitter.
        - Alfter the Farm object is created, you can run the Farm calling the "run_and_wait()" method

    This code can be executed in different ways using 'mpirun' or 'mpiexec' (-np argument must be >= 3):
        $ mpiexec -np 4 python3 app.py
        or
        $ mpiexec -np 4 ./app.py
    Warning: if both mpirun and mpiexec are not recognized, make sure you have installed OpenMPI.
        - Ubuntu command line installation: 
            $ sudo apt install openmpi-bin
    
"""
#!/usr/bin/env python3

import farm as ssp

class Emitter(ssp.SSP_Emitter):
    def code(self) -> None:
        # sending how many task you need
        for i in range (1,10):
            # print("Sending:", i)
            self.emmit(ssp.SSP_task(i))
        # advertise the computation has finished with EOS (end of stream)
        self.emmit(ssp.SSP_task(None,True))


class Worker(ssp.SSP_Worker):
    def code(self, task: ssp.SSP_task) -> None:
        # print("Computing:", task)
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
            print("Result:", task)
        # cleaning data (good practice)
        del task

def main():
    ssp._DEBUG=False
    # creating the Farm topology.
    FARM=ssp.Farm(Emitter(),Worker(),Collector(),False,False)
    # executing the Farm topology
    FARM.run_and_wait()
    # cleaning data (good practice)
    del FARM

if __name__ == "__main__":
    main()