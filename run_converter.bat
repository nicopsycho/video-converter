@echo off

REM Script to run the video converter application on Windows
REM Explicitly using the venv Python executable to ensure correct environment activation

echo Starting video converter...
g:/GIT/video-converter/.venv/Scripts/python.exe video_converter.py
pause