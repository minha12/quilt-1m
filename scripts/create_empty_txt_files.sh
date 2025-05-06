#!/bin/bash

# Configuration
MISSING_LIST="missing_txt_files.txt"
VERBOSE=1  # 0=quiet, 1=basic progress, 2=detailed

# Check if the missing files list exists
if [[ ! -f "$MISSING_LIST" ]]; then
    echo "Error: Missing files list '$MISSING_LIST' not found."
    exit 1
fi

# Count total files to process
TOTAL_FILES=$(wc -l < "$MISSING_LIST")
echo "Found $TOTAL_FILES images without text files to process..."

# Create empty text files for all missing entries
COUNTER=0
while IFS= read -r img_file; do
    # Extract the base name without extension
    txt_file="${img_file%.*}.txt"
    
    # Create empty text file
    touch "$txt_file"
    
    # Update counter and show progress
    ((COUNTER++))
    if [[ $VERBOSE -gt 0 && $((COUNTER % 100)) -eq 0 ]]; then
        echo "Progress: $COUNTER/$TOTAL_FILES files processed"
    fi
    
    # Show detailed progress
    if [[ $VERBOSE -gt 1 ]]; then
        echo "Created: $txt_file"
    fi
done < "$MISSING_LIST"

echo "Done! Created $COUNTER empty text files."
echo "You may want to check a few of the created files to verify:"
echo "ls -la $(head -n1 "$MISSING_LIST" | sed 's/\.[^.]*$/.txt/')"