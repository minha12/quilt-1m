# %%
import os
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))# %%

# %%
import pandas as pd
import os

# Load the CSV file
df = pd.read_csv('quilt_1M_lookup.csv')
# %%
df.head()
# %%

# %% Count the number of lines in the CSV file
# Method 1: Using pandas DataFrame length
num_rows_pandas = len(df)
print(f"Number of rows (excluding header) using pandas: {num_rows_pandas}")
