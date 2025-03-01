from zipfile import ZipFile
import time

# read in r/place data
def unzip(file_path):
    # loading the temp.zip and creating a zip object 
    with ZipFile(file_path, 'r') as zObject: 
        # Extracting all the members of the zip  
        zObject.extractall()
    
    return


def main():
    # start time
    start_time = time.perf_counter_ns()

    # grab unzip time
    unzip("archive.zip")

    # end time
    end_time = time.perf_counter_ns()

    # print exec time
    print(f"Execution time: {((end_time - start_time)//1000000)} ms")

if __name__ == '__main__':
    main()