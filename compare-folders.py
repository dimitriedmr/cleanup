from argparse import ArgumentParser
from csv import writer
from hashlib import md5
from multiprocessing import Process, Queue, Manager, cpu_count
from os import listdir
from os.path import join, isdir, exists, abspath, dirname, getsize
from sys import argv
from time import sleep

kbit = 1024
file_chunk_size = 8 * kbit # bits
file_ignore_over_size = 50 * kbit * kbit # Bytes

def compute(path, queue, all_files_dict):
    """Worker function to compute files hash.
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
                    chunk = f.read(file_chunk_size)
                    while chunk: # first chunks determines hash value
                        hash.update(chunk)
                        chunk = f.read(file_chunk_size)
                        
                        if getsize(file_path) > file_ignore_over_size: # in Bytes
                            chunk = None
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
    done = False
    while not done:
        if queue.empty():
            sleep(1) # 1s
        if queue.empty():
            done = True
        else:
            compute(queue.get(), queue, all_files_dict)

def save(files_path:list):
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
        for key, value in files_path.items():
            w.writerow([key, value])

if __name__ == "__main__":
    
    parser = ArgumentParser(description="Check for duplicates for a given list of folders")
    parser.add_argument("-d", action="extend", nargs="+", type=str)
    args = parser.parse_args()

    if len(argv) <= 1:
        print("It needs a directory as argument. E.g. \"-d C:\\\"")
    else:
        # shared queue with directories to explore
        queue = Queue() 
        for path in args.d:
            queue.put(path)
        # need a manager to share results dictionary
        manager = Manager()
        results = manager.dict() 
        # create workers
        processes = []
        for _ in range(cpu_count()):
            process = Process(target=worker, args=(queue, results))
            processes.append(process)
            process.start()
        # wait for all processes to finish
        for process in processes:
            process.join()

        save(results)
