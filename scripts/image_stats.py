#!/usr/bin/env python3
import os
import argparse
import multiprocessing as mp
from PIL import Image, UnidentifiedImageError
import numpy as np
import json
from tqdm import tqdm
import time
from functools import partial
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_image_size(img_path):
    """Get the size of an image."""
    try:
        with Image.open(img_path) as img:
            return img.size  # Returns (width, height)
    except (UnidentifiedImageError, IOError, OSError) as e:
        logger.warning(f"Error processing {img_path}: {e}")
        return None

def process_batch(file_batch, base_dir):
    """Process a batch of image files and return their sizes."""
    results = []
    for file_path in file_batch:
        if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp')):
            full_path = os.path.join(base_dir, file_path)
            size = get_image_size(full_path)
            if size:
                results.append((size[0], size[1]))  # (width, height)
    return results

def find_image_files(directory):
    """Generator function to yield image file paths."""
    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'}
    
    for root, _, files in os.walk(directory):
        for file in files:
            ext = os.path.splitext(file)[1].lower()
            if ext in image_extensions:
                yield os.path.join(root, file)

def calculate_running_stats(stats_dict, new_widths, new_heights):
    """Update running statistics with new batches of data."""
    # First time initialization
    if not stats_dict:
        stats_dict.update({
            'width_sum': sum(new_widths),
            'height_sum': sum(new_heights),
            'width_squared_sum': sum(w*w for w in new_widths),
            'height_squared_sum': sum(h*h for h in new_heights),
            'count': len(new_widths),
            'min_width': min(new_widths, default=float('inf')),
            'max_width': max(new_widths, default=0),
            'min_height': min(new_heights, default=float('inf')),
            'max_height': max(new_heights, default=0),
            'widths': new_widths,
            'heights': new_heights
        })
        return stats_dict
    
    # Update running sums and counts
    stats_dict['width_sum'] += sum(new_widths)
    stats_dict['height_sum'] += sum(new_heights)
    stats_dict['width_squared_sum'] += sum(w*w for w in new_widths)
    stats_dict['height_squared_sum'] += sum(h*h for h in new_heights)
    stats_dict['count'] += len(new_widths)
    
    # Update min/max
    stats_dict['min_width'] = min(stats_dict['min_width'], *new_widths)
    stats_dict['max_width'] = max(stats_dict['max_width'], *new_widths)
    stats_dict['min_height'] = min(stats_dict['min_height'], *new_heights)
    stats_dict['max_height'] = max(stats_dict['max_height'], *new_heights)
    
    # Append to lists for median calculation
    stats_dict['widths'].extend(new_widths)
    stats_dict['heights'].extend(new_heights)
    
    return stats_dict

def process_images_parallel(directory, batch_size=1000, num_workers=None):
    """Process images in parallel using multiprocessing."""
    if num_workers is None:
        num_workers = mp.cpu_count()
    
    logger.info(f"Scanning directory for image files: {directory}")
    start_time = time.time()
    
    # Get list of all image files
    all_files = list(find_image_files(directory))
    logger.info(f"Found {len(all_files)} image files. Starting processing...")
    
    # Create batches
    batches = [all_files[i:i + batch_size] for i in range(0, len(all_files), batch_size)]
    logger.info(f"Created {len(batches)} batches of size {batch_size}")
    
    # Process batches in parallel
    results = []
    stats = {}
    process_func = partial(process_batch, base_dir='')  # We're using full paths
    
    with mp.Pool(num_workers) as pool:
        for batch_results in tqdm(pool.imap(process_func, batches), total=len(batches)):
            if batch_results:
                widths, heights = zip(*batch_results)
                stats = calculate_running_stats(stats, widths, heights)
    
    # Calculate final statistics
    count = stats['count']
    if count > 0:
        mean_width = stats['width_sum'] / count
        mean_height = stats['height_sum'] / count
        
        # Variance and std dev
        var_width = (stats['width_squared_sum'] / count) - (mean_width ** 2)
        var_height = (stats['height_squared_sum'] / count) - (mean_height ** 2)
        std_width = np.sqrt(var_width)
        std_height = np.sqrt(var_height)
        
        # Calculate median (this could be memory intensive for very large datasets)
        median_width = np.median(stats['widths'])
        median_height = np.median(stats['heights'])

        final_stats = {
            'count': count,
            'width': {
                'min': stats['min_width'],
                'max': stats['max_width'],
                'mean': mean_width,
                'median': median_width,
                'std': std_width
            },
            'height': {
                'min': stats['min_height'],
                'max': stats['max_height'],
                'mean': mean_height,
                'median': median_height,
                'std': std_height
            },
            'processing_time_seconds': time.time() - start_time
        }
        
        # Clear the large lists from memory
        del stats['widths']
        del stats['heights']
        
        return final_stats
    else:
        logger.warning("No valid images were processed.")
        return None

def process_images_memory_efficient(directory, batch_size=1000, num_workers=None, sample_rate=1.0):
    """
    Process images with lower memory usage by not storing all widths/heights.
    Uses reservoir sampling for approximate median calculation.
    """
    if num_workers is None:
        num_workers = mp.cpu_count()
    
    logger.info(f"Scanning directory for image files: {directory}")
    start_time = time.time()
    
    # Initialize tracking variables
    count = 0
    width_sum = 0
    height_sum = 0
    width_squared_sum = 0
    height_squared_sum = 0
    min_width = float('inf')
    max_width = 0
    min_height = float('inf')
    max_height = 0
    
    # Reservoir sampling for approximate median (10,000 samples)
    reservoir_size = 10000
    width_samples = []
    height_samples = []
    
    # Process files in batches
    batch = []
    
    for path in tqdm(find_image_files(directory)):
        # Sample based on rate
        if np.random.random() > sample_rate:
            continue
            
        batch.append(path)
        
        if len(batch) >= batch_size:
            process_func = partial(process_batch, base_dir='')  # Using full paths
            with mp.Pool(num_workers) as pool:
                batch_results = pool.map(process_func, [batch])[0]
                
            if batch_results:
                batch_widths, batch_heights = zip(*batch_results)
                
                # Update running statistics
                batch_count = len(batch_widths)
                count += batch_count
                width_sum += sum(batch_widths)
                height_sum += sum(batch_heights)
                width_squared_sum += sum(w*w for w in batch_widths)
                height_squared_sum += sum(h*h for h in batch_heights)
                
                min_width = min(min_width, min(batch_widths))
                max_width = max(max_width, max(batch_widths))
                min_height = min(min_height, min(batch_heights))
                max_height = max(max_height, max(batch_heights))
                
                # Update reservoir samples for median approximation
                for w, h in zip(batch_widths, batch_heights):
                    if len(width_samples) < reservoir_size:
                        width_samples.append(w)
                        height_samples.append(h)
                    else:
                        # Replace elements with decreasing probability
                        r = np.random.randint(0, count)
                        if r < reservoir_size:
                            width_samples[r] = w
                            height_samples[r] = h
            
            batch = []
    
    # Process remaining files
    if batch:
        process_func = partial(process_batch, base_dir='')
        with mp.Pool(num_workers) as pool:
            batch_results = pool.map(process_func, [batch])[0]
            
        if batch_results:
            batch_widths, batch_heights = zip(*batch_results)
            
            # Update running statistics
            batch_count = len(batch_widths)
            count += batch_count
            width_sum += sum(batch_widths)
            height_sum += sum(batch_heights)
            width_squared_sum += sum(w*w for w in batch_widths)
            height_squared_sum += sum(h*h for h in batch_heights)
            
            min_width = min(min_width, min(batch_widths))
            max_width = max(max_width, max(batch_widths))
            min_height = min(min_height, min(batch_heights))
            max_height = max(max_height, max(batch_heights))
            
            # Update reservoir samples for median approximation
            for w, h in zip(batch_widths, batch_heights):
                if len(width_samples) < reservoir_size:
                    width_samples.append(w)
                    height_samples.append(h)
                else:
                    # Replace elements with decreasing probability
                    r = np.random.randint(0, count)
                    if r < reservoir_size:
                        width_samples[r] = w
                        height_samples[r] = h
    
    # Calculate final statistics
    if count > 0:
        mean_width = width_sum / count
        mean_height = height_sum / count
        
        # Variance and std dev
        var_width = (width_squared_sum / count) - (mean_width ** 2)
        var_height = (height_squared_sum / count) - (mean_height ** 2)
        std_width = np.sqrt(var_width)
        std_height = np.sqrt(var_height)
        
        # Calculate approximate median from reservoir
        median_width = np.median(width_samples) if width_samples else None
        median_height = np.median(height_samples) if height_samples else None
        
        final_stats = {
            'count': count,
            'width': {
                'min': min_width,
                'max': max_width,
                'mean': mean_width,
                'median': median_width,
                'std': std_width
            },
            'height': {
                'min': min_height,
                'max': max_height,
                'mean': mean_height,
                'median': median_height,
                'std': std_height
            },
            'processing_time_seconds': time.time() - start_time,
            'sampled': sample_rate < 1.0
        }
        
        return final_stats
    else:
        logger.warning("No valid images were processed.")
        return None

def main():
    parser = argparse.ArgumentParser(description='Calculate image dimension statistics for a dataset.')
    parser.add_argument('directory', help='Directory containing image files')
    parser.add_argument('--batch-size', type=int, default=1000, help='Number of images to process in each batch')
    parser.add_argument('--workers', type=int, default=None, help='Number of worker processes (default: number of CPU cores)')
    parser.add_argument('--output', default='image_stats.json', help='Output JSON file path')
    parser.add_argument('--memory-efficient', action='store_true', help='Use memory-efficient processing')
    parser.add_argument('--sample-rate', type=float, default=1.0, help='Fraction of images to sample (0.0-1.0)')
    
    args = parser.parse_args()
    
    logger.info(f"Starting image statistics calculation on: {args.directory}")
    logger.info(f"Using {args.workers if args.workers else mp.cpu_count()} worker processes")
    
    if args.memory_efficient:
        stats = process_images_memory_efficient(
            args.directory, 
            batch_size=args.batch_size, 
            num_workers=args.workers,
            sample_rate=args.sample_rate
        )
    else:
        stats = process_images_parallel(
            args.directory, 
            batch_size=args.batch_size, 
            num_workers=args.workers
        )
    
    if stats:
        with open(args.output, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Results saved to {args.output}")
        
        # Print summary
        logger.info("Image Statistics Summary:")
        logger.info(f"Total images processed: {stats['count']}")
        logger.info(f"Width - Min: {stats['width']['min']}, Max: {stats['width']['max']}, Mean: {stats['width']['mean']:.2f}, Median: {stats['width']['median']:.2f}")
        logger.info(f"Height - Min: {stats['height']['min']}, Max: {stats['height']['max']}, Mean: {stats['height']['mean']:.2f}, Median: {stats['height']['median']:.2f}")
        logger.info(f"Processing time: {stats['processing_time_seconds']:.2f} seconds")
    
if __name__ == "__main__":
    main()