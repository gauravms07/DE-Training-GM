import pandas as pd
import time
start = time.time()
reader = pd.read_csv('hw_200.csv', chunksize=10000)

row_count = 0

for chunk in reader:
    row_count += len(chunk)

print("Row Count: ", row_count)
print("Chunked Read Time: ", time.time()-start)