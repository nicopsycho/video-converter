#!/bin/bash

# Script to run the video converter application on Linux/macOS

# Check if the Python executable is available
if ! command -v python3 &> /dev/null
then
    echo "Error: python3 could not be found. Please ensure Python 3 is installed and in your PATH."
    exit 1
fi

# Execute the main application script
echo "Starting video converter..."
python3 video_converter.py