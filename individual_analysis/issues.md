# Issues
- Shrinking the data down from 42 GB to something a little more manageable for my weakling windows machines took a little effort, seeing as I only wanted to unzip and grab part of the CSV. In this way, I created a sort of batched reader to only grab as many GB as I ask it to. This took 449840 ms for 22 GB (just for unzipping to CSV).
- There are many columns that provide an opportunity to reduce the size of the data with little to no lossiness...
- I continuously received the following error: `Error processing row: In CSV column #23: CSV conversion error to null: invalid value '浙江'`