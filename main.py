from mpi4py import MPI
import numpy as np
import time,threading

FILE_NAME="train_split_00.csv"
#FILE_NAME="sample.txt"



amode = MPI.MODE_RDONLY
comm = MPI.COMM_WORLD
fh = MPI.File.Open(comm, FILE_NAME, amode)
rank = comm.Get_rank()

item_search = None




def master_function(name) :
    completed_process = 0

    running = True
   
    while running :
        data = comm.recv(source=MPI.ANY_SOURCE,tag=0)

        completed_process += 1

        if data["index"] ==-1 :
            print(f" Message recived from {data['rank']}: String not found")
        else:
            print(f" Message recived from {data['rank']}: String found at index {data['index']}")
        
        if completed_process == comm.size:
            
            for i in range(1,comm.size):

                comm.send("STOP",dest=i,tag=1)
            # running = run_process(rank,True,running)
            running = False
    print("exit ",running)
            
    exit(0)

def slave_funtion(name):
    running = True
    while running:
        data = comm.recv(source=0,tag=1)
        
        # print(running)
        if data == "STOP" :
            print("Message recived from 0 to stop the process with rank",rank)
            running = False
    #Exit the process
    exit(0)


#Master process
if rank == 0:
    item_search = input("Enter the string to search : ")


    x = threading.Thread(target=master_function, args=(1,))
    x.start()
else:
    item_search = None
    x = threading.Thread(target=slave_funtion, args=(1,))
    x.start()



#Broadcasting the item to search to all other nodes 
item_search = comm.bcast(item_search,root=0)
        

buffer = np.empty((fh.size//comm.size), dtype=np.int32)
buffer[:] = rank

offset = rank*buffer.nbytes
fh.Read_at_all(offset, buffer)

string = buffer.tobytes().decode()
index = string.find(item_search)

if index != -1:
    comm.send({"index": index,"rank": rank},dest=0,tag=0)
else:
    comm.send({"index": index,"rank": rank},dest=0,tag=0)



fh.Close()
