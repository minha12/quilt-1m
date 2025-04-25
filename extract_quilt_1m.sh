#!/bin/bash

# Loop through all image zip files and extract them one by one
for zip_file in images_part_*.zip; do
    echo "Starting extraction of $zip_file..."
    unzip -q "$zip_file"
    echo "Finished extracting $zip_file"
done

echo "All extraction jobs completed!"
