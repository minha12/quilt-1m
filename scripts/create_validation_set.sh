#!/bin/bash

# Configuration
SOURCE_DIR="quilt_1m"
DEST_DIR="quilt_1m_val"
PERCENTAGE=5
SEED=42  # Set a fixed seed for reproducibility

# Create destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

# Step 1: Find all jpg files and count them
echo "Counting JPG files in $SOURCE_DIR..."
TOTAL_JPGS=$(find "$SOURCE_DIR" -maxdepth 1 -name "*.jpg" | wc -l)
SAMPLE_SIZE=$((TOTAL_JPGS * PERCENTAGE / 100))

echo "Total JPG files: $TOTAL_JPGS"
echo "Will select $SAMPLE_SIZE files (${PERCENTAGE}%)"

# Step 2: Get all jpg basenames and randomly select SAMPLE_SIZE of them
echo "Randomly selecting files..."

# Fix: Use a simpler approach with shuf's built-in seed option
find "$SOURCE_DIR" -maxdepth 1 -name "*.jpg" -printf "%f\n" | 
    sed 's/\.jpg$//' | 
    shuf -n "$SAMPLE_SIZE" --random-source=/dev/urandom > selected_files.txt

# Check if we got any files
file_count=$(wc -l < selected_files.txt)
echo "Selected $file_count files"

if [ "$file_count" -eq 0 ]; then
    echo "Error: No files were selected. Exiting."
    exit 1
fi

# Step 3: Move selected files and their matching text files
echo "Moving files to $DEST_DIR..."
MOVED_PAIRS=0

while IFS= read -r basename; do
    # Debug output
    echo "Processing: $basename"
    
    # Check if both jpg and txt files exist
    if [[ -f "$SOURCE_DIR/$basename.jpg" && -f "$SOURCE_DIR/$basename.txt" ]]; then
        # Move the files
        mv "$SOURCE_DIR/$basename.jpg" "$DEST_DIR/"
        mv "$SOURCE_DIR/$basename.txt" "$DEST_DIR/"
        ((MOVED_PAIRS++))
        
        # Print progress every 100 files
        if ((MOVED_PAIRS % 100 == 0)); then
            echo "Moved $MOVED_PAIRS pairs so far..."
        fi
    else
        echo "Warning: Missing files for $basename"
        echo "  JPG exists: $([[ -f "$SOURCE_DIR/$basename.jpg" ]] && echo "Yes" || echo "No")"
        echo "  TXT exists: $([[ -f "$SOURCE_DIR/$basename.txt" ]] && echo "Yes" || echo "No")"
    fi
done < selected_files.txt

# Clean up
rm selected_files.txt

# Step 4: Print summary
echo "==============================================="
echo "Completed moving files to validation set"
echo "Total pairs moved: $MOVED_PAIRS/$SAMPLE_SIZE"
echo "Source directory: $SOURCE_DIR"
echo "Destination directory: $DEST_DIR"
echo "==============================================="