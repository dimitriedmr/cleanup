from csv import writer
from hashlib import md5
from multiprocessing import Process, Queue, Manager, cpu_count
from os import listdir
from os.path import join, isdir, exists, abspath, dirname

file_chunks_used = 20
file_chunk_size = 8192

def compute(path, queue, all_files_dict):
    """Worker function to compute has of files.
    input: path
    output: queue, all_files_dict
    """
    try:
        for entry in listdir(path):
            file_path = join(path, entry)
            if isdir(file_path):
                queue.put(file_path)  # add directories to be explored
            else:
                # to compute file hash, use several chunks from it
                hash = md5()
                with open(file_path, 'rb') as f:
                    i = 0
                    chunk = f.read(file_chunk_size)
                    while i < file_chunks_used and chunk: # first chunks determines hash value
                        hash.update(chunk)
                        i += 1
                        chunk = f.read(file_chunk_size)
                hash = hash.hexdigest()
                
                # update dict, merge based on keys
                value = file_path.encode('utf-8').decode('utf-8')
                if hash in all_files_dict:
                    all_files_dict[hash] += [value]
                else:
                    all_files_dict[hash] = [value]
    except PermissionError as e:
        print(f"Can't access directory: {e}")
        pass

def worker(queue, all_files_dict):
    while not queue.empty():
        compute(queue.get(), queue, all_files_dict)

start_paths=[
    # r"D:\test1", 
    # r"E:\test2"
    ]
    
if __name__ == "__main__":
    queue = Queue() # it contains directories to explore
    for path in start_paths:
        queue.put(path)

    manager = Manager()
    all_files_dict = manager.dict() # need a manager to share dictionary

    # create workers
    processes = []
    for _ in range(cpu_count()):
        process = Process(target=worker, args=(queue,all_files_dict))
        processes.append(process)
        process.start()
    # wait for all processes to finish
    for process in processes:
        process.join()

    # save results dictionary to csv
    out_path = join(dirname(abspath(__file__)), r"output.csv")
    sep = '.'
    i = 0
    while exists(out_path):
        p,ext = out_path.split(sep)
        out_path = p + str(i) + sep + ext
    print(out_path)

    with open(out_path, mode="w", newline="", encoding='utf8') as file:
        w = writer(file)
        w.writerow(["checksum", "paths"])
        for key, value in all_files_dict.items():
            w.writerow([key, value])
