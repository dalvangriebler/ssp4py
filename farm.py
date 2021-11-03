"""This module provides a parallel programming abstraction for the Farm pattern.
   Make sure you have installed the mpi4py dependency.
    - For Anaconda users:
        $ conda install -c conda-forge mpi4py
    - For Pip users:
        $ python -m pip install mpi4py
   For any issue related to this module, contact-me: dalvangriebler@gmail.com
"""

from abc import ABCMeta, abstractclassmethod
from mpi4py import MPI

# constants for defining message tags
_EOS_TAG=10
_DATA_TAG=11
# enable or disable debug prints
_DEBUG=True
class ErrorInvalidNumProcFarm(Exception):
    """ We need at least 3 process in the Farm for Emitter, Worker and Collector """
    def __str__(self) -> str:
        return "Expected $ mpirun -np >= 3"

class SSP_task:
    """This classe is used to represent a generic task.
    It is used to manage communication issues"""
    __EOS=False
    __task_id=0
    def __init__(self, d=None, e=False) -> None:
        self.data=d
        self.EOS=e
    @property
    def EOS(self):
        return self.__EOS
    @EOS.setter
    def EOS(self,val):
        self.__EOS=val
    @property
    def data(self):
        return self.__data
    @data.setter
    def data(self,val):
        self.__data=val
    @property
    def task_id(self):
        return self.__task_id
    
    def __str__(self):
        return str(self.__data)


class SSP_Emitter(metaclass=ABCMeta):
    """Abstract base class to be extend and implement the code method.
    This class represents the Emitter entity of the Farm pattern"""
    __id=1
    __EOS=False
    __comm=MPI.COMM_WORLD
    __num_proc=__comm.Get_size()-1
    @classmethod
    def emmit(self, task: SSP_task) -> None:
        if _DEBUG: print("Emitter-Task: ", task)
        if task.EOS:
            for id in range(2,self.__num_proc+1):
                self.__comm.send(task,dest=id, tag=_EOS_TAG)
            self.EOS=True  
        elif self.__id < self.__num_proc:
            self.__id=self.__id+1
            if _DEBUG: print("Emitter destination:",self.__id)
            self.__comm.send(task,dest=self.__id, tag=_DATA_TAG)
        else:
            self.__id=2
            self.__comm.send(task,dest=self.__id, tag=_DATA_TAG)
        del task
        
    @property
    def EOS(self):
        return self.__EOS
    @EOS.setter
    def EOS(self, val):
        self.__EOS=val
    @abstractclassmethod
    def code(self) -> None:
        pass 

class SSP_Worker(metaclass=ABCMeta):
    """Abstract base class to be extend and implement the code method.
    This class represents the Worker entity of the Farm pattern"""
    __comm=MPI.COMM_WORLD
    @classmethod
    def emmit(self, task: SSP_task) -> None:
        if _DEBUG: print("Worker-Task: ", task)
        if task.EOS:
            self.__comm.send(task,dest=1, tag=_EOS_TAG)
        else:
            self.__comm.send(task,dest=1, tag=_DATA_TAG)
        del task

    @abstractclassmethod
    def code(self, task: SSP_task) -> None:
        pass
class SSP_Collector(metaclass=ABCMeta):
    """Abstract base class to be extend and implement the code method.
    This class represents the Collector entity of the Farm pattern"""
    @abstractclassmethod
    def code(self, task: SSP_task) -> None:
        pass


class Farm:
    """This class represents the Farm pattern.
    It creates the Farm topology and implements all communication."""
    __comm=MPI.COMM_WORLD
    __my_rank = __comm.Get_rank()
    __name_proc = MPI.Get_processor_name()
    __num_proc = __comm.Get_size()
    __status_mpi = MPI.Status()
    
    def __init__(self,E,W,C,SCHE,ORD) -> None:
        if isinstance(E,SSP_Emitter):
            self.__emitter=E
        else:
            raise TypeError ("Expected object SSP_Emitter")
        if isinstance(W,SSP_Worker):
            self.__worker=W
        else:
            raise TypeError ("Expected object SSP_Worker")
        if isinstance(C,SSP_Collector):
            self.__collector=C
        else:
            raise TypeError ("Expected object SSP_Collector")
        if isinstance (SCHE,bool):
            self.__scheduler=SCHE
        else:
            raise TypeError ("Expected boolean to enable/disable scheduling")
        if isinstance (ORD,bool):
            self.__ORD=ORD
        else:
            raise TypeError ("Expected boolean to enable/disable ordering")

    def __run(self):
        # emitter
        if self.__my_rank == 0:
            while not self.__emitter.EOS:
                self.__emitter.code()
            
        # collector   
        elif self.__my_rank == 1:
            eos=self.__num_proc-2
            if _DEBUG: print("Number of EOS expetected:", eos)
            while eos>0:
                task=self.__comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=self.__status_mpi)
                if _DEBUG:
                    source = self.__status_mpi.Get_source()
                    tag = self.__status_mpi.Get_tag()
                    print("Collector-Task: ", task, " source=", source, " tag=", tag)
                if task.EOS:
                    eos=eos-1 
                else:
                    self.__collector.code(task)
        # worker
        else:
            while True:
                task=self.__comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=self.__status_mpi)
                if _DEBUG:
                    source = self.__status_mpi.Get_source()
                    tag = self.__status_mpi.Get_tag()
                    print("Worker-Task: ", task, " source=", source, " tag=", tag)
                self.__worker.code(task)
                if task.EOS: break
  
    def __start_mpi(self):
        if _DEBUG:
            print("Start MPI")      
            print("my_rank:", self.__my_rank)
            print("name_proc:", self.__name_proc)
            print("num_proc:", self.__num_proc)
        if self.__num_proc < 3:
            raise ErrorInvalidNumProcFarm()
        self.__run()
    
    def __end_mpi(self):
        MPI.Finalize()

    def run_and_wait(self):
        self.__start_mpi()
        self.__end_mpi()

if __name__ == "__main__":
    print("This is module and can not be executed as a script or program")