"""
Optimized data processing module using pandas and multiprocessing
"""

import pandas as pd
import numpy as np
import multiprocessing as mp
from functools import partial
import logging
import os
import mmap
from typing import List, Dict, Any, Tuple, Optional
from collections import Counter
import chardet

logger = logging.getLogger(__name__)

class OptimizedDataProcessor:
    """High-performance data processor for large CSV files"""
    
    def __init__(self, chunk_size: int = 10000, n_workers: Optional[int] = None):
        """
        Initialize the data processor
        
        Args:
            chunk_size: Number of rows to process in each chunk
            n_workers: Number of worker processes (defaults to CPU count)
        """
        self.chunk_size = chunk_size
        self.n_workers = n_workers or min(mp.cpu_count(), 4)  # Cap at 4 for memory efficiency
        
    def detect_file_info(self, file_path: str) -> Dict[str, Any]:
        """
        Detect file encoding, delimiter, and basic info using memory mapping
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dictionary with file information
        """
        try:
            # Use memory mapping for large file detection
            with open(file_path, 'rb') as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmapped_file:
                    # Read first chunk for detection
                    sample_size = min(10240, len(mmapped_file))  # 10KB sample
                    sample_bytes = mmapped_file[:sample_size]
                    
                    # Detect encoding with better fallback handling
                    encoding_result = chardet.detect(sample_bytes)
                    detected_encoding = encoding_result.get('encoding', 'utf-8')
                    confidence = encoding_result.get('confidence', 0.0)
                    
                    # Use UTF-8 as default for low confidence detections
                    if confidence < 0.7 or detected_encoding is None:
                        encoding = 'utf-8'
                    else:
                        encoding = detected_encoding
                    
                    # Decode sample for delimiter detection with multiple fallbacks
                    sample_text = None
                    encoding_attempts = [encoding, 'utf-8', 'latin1', 'cp1252']
                    
                    for attempt_encoding in encoding_attempts:
                        try:
                            sample_text = sample_bytes.decode(attempt_encoding)
                            encoding = attempt_encoding  # Use the encoding that worked
                            break
                        except UnicodeDecodeError:
                            continue
                    
                    if sample_text is None:
                        # Last resort: decode with errors='replace'
                        sample_text = sample_bytes.decode('utf-8', errors='replace')
                        encoding = 'utf-8'
                    
                    # Detect delimiter
                    delimiter = self._detect_delimiter(sample_text)
                    
                    # Get file size
                    file_size = len(mmapped_file)
                    
            return {
                'encoding': encoding,
                'delimiter': delimiter,
                'file_size': file_size,
                'confidence': encoding_result.get('confidence', 0.8)
            }
            
        except Exception as e:
            logger.error(f"Error detecting file info: {str(e)}")
            return {
                'encoding': 'utf-8',
                'delimiter': ',',
                'file_size': 0,
                'confidence': 0.5
            }
    
    def _detect_delimiter(self, sample_text: str) -> str:
        """Detect CSV delimiter from sample text"""
        delimiters = [',', ';', '\t', '|']
        delimiter_counts = {}
        
        lines = sample_text.split('\n')[:5]  # Check first 5 lines
        
        for delimiter in delimiters:
            count = sum(line.count(delimiter) for line in lines)
            delimiter_counts[delimiter] = count
            
        return max(delimiter_counts.items(), key=lambda x: x[1])[0]
    
    def read_csv_optimized(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        Read CSV file using pandas with optimizations
        
        Args:
            file_path: Path to CSV file
            **kwargs: Additional pandas read_csv parameters
            
        Returns:
            DataFrame with the CSV data
        """
        file_info = self.detect_file_info(file_path)
        
        # Default parameters for optimization
        default_params = {
            'encoding': file_info['encoding'],
            'sep': file_info['delimiter'],
            'low_memory': False,  # Better type inference
            'dtype': str,  # Keep all as strings for validation
            'na_filter': False,  # Don't convert to NaN
            'skip_blank_lines': True
        }
        
        # Update with user parameters
        default_params.update(kwargs)
        
        try:
            # For very large files, use chunked reading
            if file_info['file_size'] > 100 * 1024 * 1024:  # 100MB threshold
                logger.info(f"Large file detected ({file_info['file_size'] / 1024 / 1024:.1f}MB), using chunked reading")
                return self._read_large_file_chunked(file_path, default_params)
            else:
                return pd.read_csv(file_path, **default_params)
                
        except (UnicodeDecodeError, Exception) as e:
            logger.error(f"Error reading CSV with pandas: {str(e)}")
            # Try with different encodings
            encoding_attempts = ['utf-8', 'latin1', 'cp1252']
            
            for encoding in encoding_attempts:
                try:
                    fallback_params = {
                        'encoding': encoding,
                        'sep': file_info['delimiter'],
                        'dtype': str,
                        'na_filter': False,
                        'low_memory': False
                    }
                    return pd.read_csv(file_path, **fallback_params)
                except Exception:
                    continue
            
            # Last resort with error handling
            fallback_params = {
                'encoding': 'utf-8',
                'encoding_errors': 'replace',
                'sep': ',',
                'dtype': str,
                'na_filter': False,
                'low_memory': False
            }
            return pd.read_csv(file_path, **fallback_params)
    
    def _read_large_file_chunked(self, file_path: str, params: Dict) -> pd.DataFrame:
        """Read large files in chunks and concatenate"""
        chunks = []
        chunk_reader = pd.read_csv(file_path, chunksize=self.chunk_size, **params)
        
        for chunk in chunk_reader:
            chunks.append(chunk)
            
        return pd.concat(chunks, ignore_index=True)
    
    def get_file_preview(self, file_path: str, n_rows: int = 10) -> Dict[str, Any]:
        """
        Get a preview of the CSV file
        
        Args:
            file_path: Path to CSV file
            n_rows: Number of rows to preview
            
        Returns:
            Dictionary with preview data and metadata
        """
        file_info = self.detect_file_info(file_path)
        
        try:
            # Read only the preview rows with encoding fallback
            encoding_attempts = [file_info['encoding'], 'utf-8', 'latin1', 'cp1252']
            df_preview = None
            
            for encoding in encoding_attempts:
                try:
                    df_preview = pd.read_csv(
                        file_path,
                        encoding=encoding,
                        sep=file_info['delimiter'],
                        nrows=n_rows,
                        dtype=str,
                        na_filter=False
                    )
                    file_info['encoding'] = encoding  # Update with working encoding
                    break
                except UnicodeDecodeError:
                    continue
            
            if df_preview is None:
                # Last resort with error handling
                df_preview = pd.read_csv(
                    file_path,
                    encoding='utf-8',
                    encoding_errors='replace',
                    sep=file_info['delimiter'],
                    nrows=n_rows,
                    dtype=str,
                    na_filter=False
                )
            
            # Get total row count efficiently
            total_rows = self._count_file_rows(file_path, file_info)
            
            return {
                'preview_data': df_preview.to_dict('records'),
                'columns': df_preview.columns.tolist(),
                'total_rows': total_rows,
                'file_info': file_info
            }
            
        except Exception as e:
            logger.error(f"Error getting file preview: {str(e)}")
            return {
                'preview_data': [],
                'columns': [],
                'total_rows': 0,
                'file_info': file_info
            }
    
    def _count_file_rows(self, file_path: str, file_info: Dict) -> int:
        """Efficiently count rows in CSV file"""
        try:
            # For small files, use pandas
            if file_info['file_size'] < 50 * 1024 * 1024:  # 50MB
                encoding_attempts = [file_info['encoding'], 'utf-8', 'latin1', 'cp1252']
                
                for encoding in encoding_attempts:
                    try:
                        df = pd.read_csv(
                            file_path,
                            encoding=encoding,
                            sep=file_info['delimiter'],
                            usecols=[0],  # Read only first column
                            dtype=str
                        )
                        return len(df)
                    except UnicodeDecodeError:
                        continue
                
                # Last resort with error handling
                df = pd.read_csv(
                    file_path,
                    encoding='utf-8',
                    encoding_errors='replace',
                    sep=file_info['delimiter'],
                    usecols=[0],
                    dtype=str
                )
                return len(df)
            else:
                # For large files, count lines manually with encoding fallback
                encoding_attempts = [file_info['encoding'], 'utf-8', 'latin1', 'cp1252']
                
                for encoding in encoding_attempts:
                    try:
                        with open(file_path, 'r', encoding=encoding) as f:
                            return sum(1 for _ in f) - 1  # Subtract header
                    except UnicodeDecodeError:
                        continue
                
                # Last resort
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    return sum(1 for _ in f) - 1
                    
        except Exception as e:
            logger.warning(f"Error counting rows: {str(e)}")
            return 0
    
    def validate_dataframe_parallel(self, df: pd.DataFrame, validation_func, 
                                  required_columns: List[str], 
                                  column_mappings: Dict[str, str] = None,
                                  regulation_info: Dict = None) -> Dict[str, Any]:
        """
        Validate DataFrame using parallel processing
        
        Args:
            df: DataFrame to validate
            validation_func: Validation function to apply
            required_columns: List of required columns to validate
            column_mappings: Column name mappings
            regulation_info: Regulation-specific information
            
        Returns:
            Dictionary with validation results
        """
        if column_mappings:
            df = df.rename(columns=column_mappings)
        
        # Split DataFrame into chunks for parallel processing
        chunks = self._split_dataframe(df, self.chunk_size)
        
        # Prepare validation function with fixed parameters
        validate_chunk = partial(
            self._validate_chunk,
            validation_func=validation_func,
            required_columns=required_columns,
            regulation_info=regulation_info
        )
        
        # Process chunks in parallel
        with mp.Pool(processes=self.n_workers) as pool:
            chunk_results = pool.map(validate_chunk, chunks)
        
        # Combine results
        return self._combine_validation_results(chunk_results)
    
    def _split_dataframe(self, df: pd.DataFrame, chunk_size: int) -> List[Tuple[pd.DataFrame, int]]:
        """Split DataFrame into chunks with starting indices"""
        chunks = []
        for start_idx in range(0, len(df), chunk_size):
            end_idx = min(start_idx + chunk_size, len(df))
            chunk_df = df.iloc[start_idx:end_idx].copy()
            chunks.append((chunk_df, start_idx))
        return chunks
    
    def _validate_chunk(self, chunk_data: Tuple[pd.DataFrame, int], 
                       validation_func, required_columns: List[str],
                       regulation_info: Dict = None) -> Dict[str, Any]:
        """Validate a single chunk of data"""
        chunk_df, start_idx = chunk_data
        
        validation_results = []
        error_counter = Counter()
        duplicate_tracking = {
            'emails': {},
            'usernames': {},
            'personalids': {},
            'idcardnos': {}
        }
        
        for idx, row in chunk_df.iterrows():
            row_dict = row.to_dict()
            global_idx = start_idx + (idx - chunk_df.index[0])
            
            # Perform validation
            result = validation_func(
                row_data=row_dict, 
                row_index=global_idx, 
                required_columns=required_columns, 
                regulation_info=regulation_info
            )
            
            validation_results.append(result)
            
            # Update counters
            if not result['valid']:
                for error in result['errors']:
                    error_counter[error] += 1
            
            # Track duplicates within chunk
            self._track_duplicates_in_chunk(row_dict, global_idx, duplicate_tracking)
        
        return {
            'results': validation_results,
            'error_counter': error_counter,
            'duplicate_tracking': duplicate_tracking,
            'start_idx': start_idx,
            'chunk_size': len(chunk_df)
        }
    
    def _track_duplicates_in_chunk(self, row: Dict, idx: int, tracking: Dict):
        """Track duplicate values within a chunk"""
        fields_to_track = ['email', 'username', 'personalid', 'idcardno']
        
        for field in fields_to_track:
            if field in row and row[field]:
                value = str(row[field]).strip().lower()
                if value:
                    field_key = f"{field}s"
                    if value in tracking[field_key]:
                        tracking[field_key][value].append(idx)
                    else:
                        tracking[field_key][value] = [idx]
    
    def _combine_validation_results(self, chunk_results: List[Dict]) -> Dict[str, Any]:
        """Combine results from parallel validation"""
        all_results = []
        combined_error_counter = Counter()
        global_duplicate_tracking = {
            'emails': {},
            'usernames': {},
            'personalids': {},
            'idcardnos': {}
        }
        
        # Combine all chunk results
        for chunk_result in chunk_results:
            all_results.extend(chunk_result['results'])
            combined_error_counter.update(chunk_result['error_counter'])
            
            # Merge duplicate tracking
            for field_key, field_tracking in chunk_result['duplicate_tracking'].items():
                for value, indices in field_tracking.items():
                    if value in global_duplicate_tracking[field_key]:
                        global_duplicate_tracking[field_key][value].extend(indices)
                    else:
                        global_duplicate_tracking[field_key][value] = indices
        
        # Find actual duplicates (values that appear more than once)
        duplicate_counts = {}
        for field_key, field_tracking in global_duplicate_tracking.items():
            duplicate_counts[field_key] = sum(
                1 for indices in field_tracking.values() if len(indices) > 1
            )
        
        # Calculate summary statistics
        valid_rows = sum(1 for result in all_results if result['valid'])
        invalid_rows = len(all_results) - valid_rows
        
        return {
            'validation_complete': True,
            'total_rows': len(all_results),
            'valid_rows': valid_rows,
            'invalid_rows': invalid_rows,
            'error_counts': dict(combined_error_counter.most_common()),
            'duplicate_counts': duplicate_counts,
            'results': all_results,
            'global_duplicates': global_duplicate_tracking
        }
    
    def get_memory_usage_estimate(self, file_path: str) -> Dict[str, Any]:
        """Estimate memory usage for processing the file"""
        file_info = self.detect_file_info(file_path)
        file_size_mb = file_info['file_size'] / (1024 * 1024)
        
        # Rough estimate: pandas uses 2-4x file size in memory
        estimated_memory_mb = file_size_mb * 3
        
        # Check if chunked processing is recommended
        available_memory_mb = 1024  # Assume 1GB available (conservative)
        use_chunked = estimated_memory_mb > available_memory_mb * 0.7
        
        return {
            'file_size_mb': round(file_size_mb, 2),
            'estimated_memory_mb': round(estimated_memory_mb, 2),
            'use_chunked_processing': use_chunked,
            'recommended_chunk_size': self.chunk_size if use_chunked else None
        }

# Global instance for use in Flask app
data_processor = OptimizedDataProcessor()
