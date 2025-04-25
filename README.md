# Quilt-1M Dataset Processing

This repository contains tools and scripts for processing the Quilt-1M dataset, a large-scale image-caption dataset containing approximately 1 million image-text pairs.

## Dataset Structure

```
quilt-1m/
├── archive/                  # Compressed dataset files
│   ├── images_part_1.zip     # Image archives split into 10 parts
│   ├── images_part_2.zip
│   ├── ...
│   ├── images_part_10.zip
│   └── quilt_instruct.zip    # Instruction data (if applicable)
├── quilt_1m/                 # Extracted raw dataset
│   ├── [ID]_image_[UUID].jpg # Original image files
├── quilt_1m_paired/          # Processed image-caption pairs
│   ├── [basename]_pair[n].jpg # Copied and renamed image files
│   └── [basename]_pair[n].txt # Generated caption files
├── notebooks/                # Analysis notebooks
│   └── main.py               # Example notebook for data exploration
├── scripts/
│   └── create_prompts.py     # Script to create paired data
├── extract_quilt_1m.sh       # Extraction script for zip archives
├── quilt_1M_lookup.csv       # Large CSV file with image paths and captions
└── requirements.txt          # Python dependencies
```

## Requirements

The following Python packages are required to run the processing scripts:

```
pandas
tqdm
```

To install these requirements, run:

```bash
pip install -r requirements.txt
```

## Processing Steps

The dataset processing involves several steps:

1. **Extract the raw dataset**: Use the extraction script to unpack the image archives
   ```bash
   bash extract_quilt_1m.sh
   ```
   This script will extract all `images_part_*.zip` files in the archive folder.

2. **Process image-caption pairs**: Create paired data using the Python script
   ```bash
   python scripts/create_prompts.py
   ```
   This script:
   - Reads the `quilt_1M_lookup.csv` file containing image paths and captions
   - Copies images from the `quilt_1m/` directory to the `quilt_1m_paired/` directory
   - Creates corresponding text files with captions in the `quilt_1m_paired/` directory

## Expected Results

After running the processing scripts:

1. The `quilt_1m_paired/` directory will contain:
   - Image files copied from the original dataset
   - Matching text files with the same base name as each image, containing its caption

2. Each image-text pair will follow the naming convention:
   - Image: `[basename]_pair[index].[ext]` 
   - Caption: `[basename]_pair[index].txt`

The script will provide a summary showing:
- Number of image-caption pairs created
- Number of original image files processed
- Number of missing image files (referenced in CSV but not found)
- Number of skipped entries (due to missing data or invalid extensions)
- Number of errors during processing

## Notes

- The CSV file contains mappings between image paths and captions
- The dataset supports various image formats: jpg, jpeg, png, tif, tiff, bmp, and gif
- Images referenced in the CSV that are not found in the `quilt_1m/` directory will be counted as missing