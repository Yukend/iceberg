import nbformat

# Specify the source and target notebook paths
source_notebook_path = 'testcases/postgres-pyspark-delta-partition.ipynb'
target_notebook_path = 'testcases/postgres-pyspark-iceberg.ipynb'

# Read the source notebook
with open(source_notebook_path, 'r', encoding='utf-8') as source_file:
    source_notebook = nbformat.read(source_file, as_version=4)

# Extract cells from index 10 to 30 (inclusive)
start_index = 17
end_index = 40
selected_cells = source_notebook['cells'][start_index:end_index + 1]

# Read the target notebook
with open(target_notebook_path, 'r', encoding='utf-8') as target_file:
    target_notebook = nbformat.read(target_file, as_version=4)

# Append the selected cells to the target notebook
target_notebook['cells'].extend(selected_cells)

# Write the modified target notebook back to the file
with open(target_notebook_path, 'w', encoding='utf-8') as target_file:
    nbformat.write(target_notebook, target_file)