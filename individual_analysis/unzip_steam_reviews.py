import zipfile
import time
import os

def unzip_limited(file_path, extract_to, max_size_gb):
    max_size_bytes = max_size_gb * (1024**3)  # Convert GB to bytes
    extracted_size = 0
    
    with zipfile.ZipFile(file_path, 'r') as zObject:
        for file_info in zObject.infolist():
            if extracted_size >= max_size_bytes:
                print(f"Reached {max_size_gb}GB limit, stopping extraction.")
                break
            
            # Full path of extracted file
            output_path = os.path.join(extract_to, file_info.filename)

            # Extract in chunks to avoid loading too much into memory
            with zObject.open(file_info) as source, open(output_path, "wb") as target:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):  # Read 1MB chunks
                    if extracted_size + len(chunk) > max_size_bytes:
                        print(f"Reached {max_size_gb}GB limit, stopping mid-file extraction.")
                        return
                    target.write(chunk)
                    extracted_size += len(chunk)
    
    print("Extraction complete.")


# # read in r/place data
# def unzip(file_path):
#     # loading the temp.zip and creating a zip object 
#     with ZipFile(file_path, 'r') as zObject: 
#         # Extracting all the members of the zip  
#         zObject.extractall()
    
#     return


def main():
    # start time
    start_time = time.perf_counter_ns()

    # grab unzip time
    # unzip("archive.zip")
    unzip_limited("archive.zip", ".", 1)

    # end time
    end_time = time.perf_counter_ns()

    # print exec time
    print(f"Execution time: {((end_time - start_time)//1000000)} ms")

if __name__ == '__main__':
    main()