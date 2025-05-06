#!/bin/bash
# Configuration
IMAGE_DIR="quilt_1m"  # Directory containing image and text files
OUTPUT_FILE="missing_txt_files.txt"  # Output file for missing TXT files
VERBOSE=1  # 0=quiet, 1=progress, 2=debug

# Check if directory exists
if [[ ! -d "$IMAGE_DIR" ]]; then
    echo "Error: Directory '$IMAGE_DIR' not found."
    exit 1
fi

echo "Checking for images without corresponding text files in $IMAGE_DIR..."

# Create a temporary file to hold results
temp_output=$(mktemp)
trap 'rm -f "$temp_output"' EXIT

# One-pass approach: Use find with -exec to check for .txt files on the spot
# This eliminates many of the parallel overheads and function calls
find "$IMAGE_DIR" \( -name "*.jpg" -o -name "*.jpeg" -o -name "*.png" -o -name "*.tiff" -o -name "*.webp" \) \
    -type f \
    -exec bash -c '
        for img; do
            txt="${img%.*}.txt"
            [[ ! -f "$txt" ]] && echo "$img"
        done
    ' _ {} + > "$temp_output"

# Move the temp file to the final output
mv "$temp_output" "$OUTPUT_FILE"

# Count missing files
MISSING_COUNT=$(wc -l < "$OUTPUT_FILE")
echo "Process complete!"
echo "Found $MISSING_COUNT images without matching text files."
echo "Missing file list saved to: $OUTPUT_FILE"

# Display a few examples if any missing files found
if [[ $MISSING_COUNT -gt 0 ]]; then
    echo
    echo "First 10 examples of missing text files:"
    head -n 10 "$OUTPUT_FILE"
    
    echo
    echo "What to do next?"
    echo "1. Review the complete list in $OUTPUT_FILE"
    echo "2. To create empty text files for all missing entries:"
    echo "   cat $OUTPUT_FILE | parallel 'touch \"{.}.txt\"'"
    echo "3. To remove images without text files:"
    echo "   cat $OUTPUT_FILE | parallel 'rm \"{}\"'"
fi