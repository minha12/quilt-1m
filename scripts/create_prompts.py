import csv
import os
import shutil
from tqdm import tqdm
import time

# --- Configuration ---
csv_file_path = 'quilt_1M_lookup.csv'    # Path to your CSV file
image_data_dir = 'quilt_1m/'             # Path to the directory containing original images
output_dir = 'quilt_1m_paired/'          # Path where duplicated images and caption files will be saved
# Define recognized image file extensions (lowercase)
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.tif', '.tiff', '.bmp', '.gif'}
# --- End Configuration ---

# --- Initialize Counters ---
pairs_created = 0
image_files_found = 0
missing_image_count = 0
errors_count = 0
skipped_count = 0

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# --- Step 1: Load CSV data and process image-caption pairs ---
print(f"Processing CSV data from '{csv_file_path}'...")

try:
    # Estimate CSV rows for progress bar
    csv_total_rows = None
    try:
        with open(csv_file_path, 'r', encoding='utf-8', errors='ignore') as f_count:
            reader_count = csv.reader(f_count)
            _ = next(reader_count)  # Skip header
            csv_total_rows = sum(1 for row in reader_count)
    except Exception as e:
        print(f"Warning: Could not determine total CSV rows for progress bar. {e}")

    # Process each row in the CSV
    with open(csv_file_path, mode='r', encoding='utf-8', newline='', errors='ignore') as infile:
        reader = csv.DictReader(infile)
        if 'caption' not in reader.fieldnames or 'image_path' not in reader.fieldnames:
            raise ValueError("CSV file must contain 'caption' and 'image_path' columns.")

        csv_iterator = tqdm(reader, total=csv_total_rows, desc="Processing image-caption pairs", unit="pair")
        for row_index, row in enumerate(csv_iterator):
            img_path = row.get('image_path', '').strip()
            caption = row.get('caption', '').strip()
            
            if not img_path or not caption:
                skipped_count += 1
                continue
                
            # Extract filename and extension
            filename = os.path.basename(img_path)
            basename, ext = os.path.splitext(filename)
            
            if ext.lower() not in IMAGE_EXTENSIONS:
                skipped_count += 1
                continue
                
            # Source file path
            source_img_path = os.path.join(image_data_dir, filename)
            
            # Check if source image exists
            if not os.path.isfile(source_img_path):
                missing_image_count += 1
                continue
                
            # Create unique identifiers for each pair
            pair_id = f"{basename}_pair{row_index}"
            dest_img_path = os.path.join(output_dir, f"{pair_id}{ext}")
            dest_txt_path = os.path.join(output_dir, f"{pair_id}.txt")
            
            try:
                # Copy the image file
                shutil.copy2(source_img_path, dest_img_path)
                
                # Create caption file with single caption
                with open(dest_txt_path, 'w', encoding='utf-8') as outfile:
                    outfile.write(caption)
                    
                pairs_created += 1
                image_files_found += 1
                
            except Exception as e:
                print(f"\nError processing pair for '{filename}': {e}")
                errors_count += 1

except FileNotFoundError:
    print(f"Error: The CSV file '{csv_file_path}' was not found.")
    exit()
except ValueError as ve:
    print(f"Error: {ve}")
    exit()
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    exit()

# --- Final Summary ---
print("\n--- Processing Summary ---")
print(f"Image-caption pairs created: {pairs_created}")
print(f"Original image files found and processed: {image_files_found}")
print(f"Missing image files (referenced in CSV but not found): {missing_image_count}")
print(f"Skipped entries (missing data or invalid extensions): {skipped_count}")
print(f"Errors during processing: {errors_count}")
print(f"Output saved to: '{output_dir}'")
print("--------------------------")