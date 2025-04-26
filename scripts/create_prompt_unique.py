import csv
import os
from tqdm import tqdm
import time

# --- Configuration ---
csv_file_path = 'quilt_1M_lookup.csv' # Path to your CSV file
image_data_dir = 'quilt_1m/'        # Path to the directory containing images AND where .txt files will be saved
# Define recognized image file extensions (lowercase)
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.tif', '.tiff', '.bmp', '.gif'}
# Separator to use between multiple captions in the output .txt file
CAPTION_SEPARATOR = "\n\n"
# --- End Configuration ---

# --- Initialize Counters ---
txt_files_created = 0
image_files_found = 0
already_exists_count = 0
missing_caption_count = 0 # Images found but not in CSV lookup
errors_writing_txt = 0
non_image_files_skipped = 0
directories_or_other_skipped = 0 # Added counter for directories

# --- Step 1: Load CSV into a lookup dictionary (storing lists of captions) ---
print(f"Loading CSV data from '{csv_file_path}' into memory...")
print("(This might take a while and consume significant RAM for large files)")
# Store captions as lists: { "filename": ["caption1", "caption2", ...] }
caption_lookup = {}
csv_rows_read = 0
try:
    # Estimate CSV rows for progress bar
    csv_total_rows = None
    try:
        with open(csv_file_path, 'r', encoding='utf-8', errors='ignore') as f_count:
            reader_count = csv.reader(f_count)
            _ = next(reader_count) # Skip header
            csv_total_rows = sum(1 for row in reader_count)
    except Exception as e:
        print(f"Warning: Could not determine total CSV rows for progress bar. {e}")

    # Read the CSV, appending captions to lists in the dictionary
    with open(csv_file_path, mode='r', encoding='utf-8', newline='', errors='ignore') as infile:
        reader = csv.DictReader(infile)
        if 'caption' not in reader.fieldnames or 'image_path' not in reader.fieldnames:
             raise ValueError("CSV file must contain 'caption' and 'image_path' columns.")

        csv_iterator = tqdm(reader, total=csv_total_rows, desc="Reading CSV", unit="row")
        for row in csv_iterator:
            csv_rows_read += 1
            img_path = row.get('image_path')
            caption = row.get('caption')
            if img_path and caption:
                filename = os.path.basename(img_path.strip())
                caption_text = caption.strip()
                # If filename is new, create a new list for it
                if filename not in caption_lookup:
                    caption_lookup[filename] = []
                # Append the current caption to the list for this filename
                caption_lookup[filename].append(caption_text)

    print(f"Finished reading {csv_rows_read} rows from CSV.")
    print(f"Stored captions for {len(caption_lookup)} unique image filenames.") # Changed wording

except FileNotFoundError:
    print(f"Error: The CSV file '{csv_file_path}' was not found.")
    exit()
except ValueError as ve:
    print(f"Error: {ve}")
    exit()
except Exception as e:
    print(f"An unexpected error occurred during CSV loading: {e}")
    exit()
# --- End Step 1 ---


# --- Step 2 & 3: Iterate through image directory and create text files ---
print(f"\nScanning image directory: '{image_data_dir}'")

if not os.path.isdir(image_data_dir):
    print(f"Error: The specified image data directory '{image_data_dir}' does not exist.")
    exit()

total_dir_entries = 0 # Initialize counter
try:
    print("Counting entries in directory for progress bar...")
    # Fast way to count entries (files+dirs)
    total_dir_entries = len(os.listdir(image_data_dir))
    print(f"Found {total_dir_entries} total entries.")

    # Use os.scandir for efficient iteration
    directory_iterator = tqdm(os.scandir(image_data_dir), total=total_dir_entries, desc="Processing directory", unit="entry")

    for entry in directory_iterator:
        if entry.is_file():
            filename = entry.name
            basename, ext = os.path.splitext(filename)

            if ext.lower() in IMAGE_EXTENSIONS:
                image_files_found += 1
                captions_list = caption_lookup.get(filename) # Get list of captions

                if captions_list: # Check if list exists and is not empty
                    dest_txt_path = os.path.join(image_data_dir, basename + '.txt')

                    # if os.path.exists(dest_txt_path):
                    #     already_exists_count += 1
                    #     continue

                    # --- Join the list of captions into one string ---
                    full_caption_text = CAPTION_SEPARATOR.join(captions_list)

                    try:
                        with open(dest_txt_path, 'w', encoding='utf-8') as outfile:
                            outfile.write(full_caption_text)
                        txt_files_created += 1
                    except OSError as oe:
                        print(f"\nOS Error writing text file '{dest_txt_path}': {oe}")
                        errors_writing_txt += 1
                    except Exception as e:
                         print(f"\nError writing text file '{dest_txt_path}': {e}")
                         errors_writing_txt += 1
                else:
                    missing_caption_count += 1 # Image found, but filename not in CSV lookup
            else:
                 non_image_files_skipped += 1 # File, but not a recognized image extension
        else:
            # Entry is not a file (it's a directory, symlink, etc.)
            directories_or_other_skipped += 1

except FileNotFoundError:
    print(f"Error: Image directory '{image_data_dir}' not found during scan.")
except Exception as e:
    print(f"\nAn error occurred during directory scan: {e}")
# --- End Step 2 & 3 ---


# --- Step 4: Final Summary ---
print("\n--- Processing Summary ---")
print(f"CSV rows read: {csv_rows_read}")
print(f"Unique image filenames with captions loaded from CSV: {len(caption_lookup)}")
print("---")
print(f"Total entries scanned in directory: {total_dir_entries}")
print(f"Image files processed (matching extensions): {image_files_found}")
print(f"Skipped non-image files: {non_image_files_skipped}")
print(f"Skipped directories or other non-files: {directories_or_other_skipped}")
print(f"  (Check: {image_files_found + non_image_files_skipped + directories_or_other_skipped} == {total_dir_entries}?)") # Verification check
print("---")
print(f"Text files created (one per image with captions): {txt_files_created}")
print(f"Skipped creating text file (already existed): {already_exists_count}")
print(f"Skipped creating text file (image found, but no caption in CSV): {missing_caption_count}")
print(f"Errors writing text files: {errors_writing_txt}")
print(f"Text files were saved within: '{image_data_dir}' using CAPTION_SEPARATOR as separator")
print("--------------------------")