from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import io
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import numpy as np
import requests
from obspy import UTCDateTime, read as obspy_read
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.header import FDSNNoDataException

def download_single_station(station, start, end, network, location, channel, 
                            starttime_override, endtime_override, output_dir, client):
    """
    Download miniseed file for a single station.
    
    This function is designed to be called in parallel for multiple stations.
    It handles all the logic for one download operation.
    
    Parameters:
    -----------
    station : str
        Station code (e.g., 'ANMO')
    start : str
        Station's start date from metadata
    end : str
        Station's end date from metadata
    network : str
        Network code (e.g., 'XD')
    location : str
        Location code (e.g., '*' for all locations)
    channel : str
        Channel code (e.g., 'BH?' for all BH channels)
    starttime_override : str or None
        Override start time if provided
    endtime_override : str or None
        Override end time if provided
    output_dir : str
        Directory to save the file
    client : obspy.clients.fdsn.Client
        FDSN client instance
    
    Returns:
    --------
    tuple : (success: bool, filepath: str or None, error: str or None)
    """
    # Determine actual start/end times (use override if provided, otherwise use station metadata)
    actual_start = starttime_override if starttime_override is not None else start
    actual_end = endtime_override if endtime_override is not None else end
    starttime = UTCDateTime(actual_start)
    endtime = UTCDateTime(actual_end)
    
    try:
        # Request waveform data from the FDSN service
        st = client.get_waveforms(
            network=network,
            station=station,
            location=location,
            channel=channel,
            starttime=starttime,
            endtime=endtime
        )
        
        # Create a descriptive filename with network, station, and date range
        filename = f"{network}_{station}_{starttime.strftime('%Y%m%d')}_{endtime.strftime('%Y%m%d')}.mseed"
        filepath = os.path.join(output_dir, filename)
        
        # Save the waveform data to a miniseed file
        st.write(filepath, format='MSEED')
        print(f"✓ Successfully saved to: {filepath}")
        
        return (True, filepath, None)
        
    except Exception as e:
        # If download fails, print error and return failure status
        print(f"✗ Failed to download {station}: {str(e)}")
        return (False, None, str(e))

def download_miniseed_parallel(station_rows, *, starttime=None, endtime=None, 
                     output_dir="./seismic_data", max_workers=5, client="https://service.earthscope.org"):
    """
    Download miniseed files from EarthScope's FDSN service in parallel.
    
    This function uses ThreadPoolExecutor to download data from multiple stations
    simultaneously, significantly reducing total download time compared to sequential
    downloads.
    
    Parameters:
    -----------
    station_rows : iterable
        Iterable of station objects with .station, .start, .end attributes
        (e.g., rows from a pandas DataFrame or list of namedtuples)
    starttime : str, optional
        Override start time in format 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS'
        If None, uses each station's individual start time
    endtime : str, optional
        Override end time in format 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS'
        If None, uses each station's individual end time
    output_dir : str, optional
        Directory to save the miniseed files (default: './seismic_data')
    max_workers : int, optional
        Maximum number of parallel downloads (default: 5)
        Increase for faster downloads, but be mindful of server limits
    
    Returns:
    --------
    dict : Dictionary containing:
        - 'successful': List of dicts with station names and filepaths
        - 'failed': List of dicts with station names and error messages
    
    Example:
    --------
    >>> # Assuming you have a DataFrame with station information
    >>> import pandas as pd
    >>> stations_df = pd.DataFrame({
    ...     'station': ['ANMO', 'CCM', 'HLID'],
    ...     'start': ['2024-01-01', '2024-01-01', '2024-01-01'],
    ...     'end': ['2024-01-02', '2024-01-02', '2024-01-02']
    ... })
    >>> 
    >>> # Download data for all stations in parallel
    >>> results = download_miniseed(
    ...     stations_df.itertuples(),
    ...     starttime='2024-01-01',
    ...     endtime='2024-01-02',
    ...     max_workers=10
    ... )
    >>> 
    >>> print(f"Downloaded {len(results['successful'])} files")
    >>> print(f"Failed: {len(results['failed'])} files")
    """
    
    # Default FDSN parameters for EarthScope network
    network = "XD"        # Network code
    location = "*"        # All locations
    channel = "HHZ"       # All broadband high-gain channels (BHZ, BHN, BHE)
    
    # Extract station data from the input rows
    # This creates a list of tuples: [(station_code, start_date, end_date), ...]
    station_data = [(row.station, row.start_date, row.end_date) for row in station_rows]
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory: {output_dir}")
    
    # Initialize EarthScope FDSN client
    # The client object is thread-safe and can be shared across threads
    client = Client(client)
    print(f"Initialized EarthScope client")
    print(f"Preparing to download {len(station_data)} stations with {max_workers} parallel workers\n")
    
    # Initialize results dictionary to track successful and failed downloads
    results = {
        'successful': [],
        'failed': []
    }
    
    # Use ThreadPoolExecutor for parallel downloads
    # ThreadPoolExecutor is ideal for I/O-bound tasks like network downloads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks to the executor
        # This creates a Future object for each station download
        future_to_station = {
            executor.submit(
                download_single_station,
                station, start, end, network, location, channel,
                starttime, endtime, output_dir, client
            ): station
            for station, start, end in station_data
        }
        
        # Process completed downloads as they finish (not in submission order)
        # as_completed() yields futures as they complete, allowing real-time progress updates
        for future in as_completed(future_to_station):
            station = future_to_station[future]
            try:
                # Get the result from the completed future
                success, filepath, error = future.result()
                
                if success:
                    # Add to successful downloads list
                    results['successful'].append({
                        'station': station,
                        'filepath': filepath
                    })
                else:
                    # Add to failed downloads list with error message
                    results['failed'].append({
                        'station': station,
                        'error': error
                    })
            except Exception as e:
                # Handle any unexpected errors that weren't caught in download_single_station
                print(f"✗ Unexpected error for station {station}: {str(e)}")
                results['failed'].append({
                    'station': station,
                    'error': str(e)
                })
    
    # Print summary of download results
    print(f"\n{'='*60}")
    print(f"Download Summary:")
    print(f"  ✓ Successful: {len(results['successful'])} stations")
    print(f"  ✗ Failed: {len(results['failed'])} stations")
    print(f"{'='*60}")
    
    return results



