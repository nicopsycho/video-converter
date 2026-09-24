import json
import os
import platform
import re
import subprocess
import typing
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

LINUX_EXECUTABLE_PATHS = {
    "ffprobe": "/mnt/g/TOOLS/ffmpeg/bin/ffprobe.exe",
    "ffmpeg": "/mnt/g/TOOLS/ffmpeg/bin/ffmpeg.exe",
    "eac3to": "/mnt/g/TOOLS/eac3to/eac3to.exe",
    "seconv": "/mnt/g/TOOLS/SubtitleEdit/seconv.exe",
    "mkvmerge": "/mnt/g/TOOLS/MKVToolNix/mkvmerge.exe"
}

WINDOWS_EXECUTABLE_PATHS = {
    "ffprobe": "G:\\TOOLS\\ffmpeg\\bin\\ffprobe.exe",
    "ffmpeg": "G:\\TOOLS\\ffmpeg\\bin\\ffmpeg.exe",
    "eac3to": "G:\\TOOLS\\eac3to\\eac3to.exe",
    "seconv": "G:\\TOOLS\\SubtitleEdit\\seconv.exe",
    "mkvmerge": "G:\\TOOLS\\MKVToolNix\\mkvmerge.exe"
}

LINUX_MEDIA_SCAN_PATHS = [
    "/mnt/d/todo",
]

WINDOWS_MEDIA_SCAN_PATHS = [
    "D:\\todo",
]

# --- Configuration and Platform Detection ---
class Config:
    """Holds all configuration, including platform-specific paths."""
    def __init__(self, scan_directory = None) -> None:
        self.platform: str = platform.system()
        # Always use the provided scan_directory or default to the current working directory (os.getcwd())
        self.executables = {}
        self.scan_paths = []
        self._load_paths()
        self._load_scan_paths()
        self._load_api_keys()
        self._load_api_keys()

    def _load_paths(self) -> None:
        # Placeholder for loading paths based on platform and user input
        # These paths must be dynamically set based on user input later.
        if self.platform == "Linux":
            self.executables = LINUX_EXECUTABLE_PATHS
        elif self.platform == "Windows":
            self.executables = WINDOWS_EXECUTABLE_PATHS

    def get_executable(self, name: str) -> str:
        return self.executables.get(name, name)

    def _load_scan_paths(self) -> None:
        if self.platform == "Linux":
            self.scan_paths = LINUX_MEDIA_SCAN_PATHS
        elif self.platform == "Windows":
            self.scan_paths = WINDOWS_MEDIA_SCAN_PATHS
        else:
            self.scan_paths = []

    def _load_api_keys(self) -> None:
        # Load from environment variables (which includes .env file via load_dotenv)
        self.tmdb_api_key = os.environ.get("TMDB_API_KEY")
        if not self.tmdb_api_key:
            print("Warning: TMDB_API_KEY not found in environment or .env file.")


# --- Core Logic Classes ---
class MediaFile:
    """Represents a discovered media file and its metadata."""
    def __init__(self, path: str, standardized_id: str, media_type: str, metadata: dict[str, typing.Any]) -> None:
        self.path: str = path
        self.standardized_id: str = standardized_id
        self.media_type: str = media_type # 'Series' or 'Movie'
        self.metadata: dict[str, typing.Any] = metadata # Contains all ffprobe data
        self.original_language: Optional[str] = None
        self.selected_audio_tracks: list[str] = []
        self.selected_subtitle_tracks: list[str] = []
        self.selected_video_tracks: list[str] = []

# --- Helper Functions ---
def get_original_language(config: Config, standardized_id: str, media_type: str, metadata: dict[str, typing.Any]) -> Optional[str]:
    """
    Attempts to fetch the original language from TMDB.
    """
    if not config.tmdb_api_key:
        print("Warning: No TMDB API key configured. Skipping language lookup.")
        return None

    # Fallback logic: if there are 2 audio languages, assume the non-French one is the original.
    audio_streams = [s for s in metadata.get('streams', []) if s.get('codec_type') == 'audio']
    if len(audio_streams) == 2:
        languages = [s.get('language') for s in audio_streams if s.get('language')]
        # Filter out 'fre' and 'fra' (French) and see what's left
        non_french = [lang for lang in languages if lang and lang.lower() not in ['fre', 'fra']]
        if len(non_french) == 1:
            return non_french[0]

    print(f"Attempting to lookup original language for {media_type}: {standardized_id}...")
    
    # Placeholder for actual API calls
    # Example: 
    # if media_type == 'Movie':
    #     resp = requests.get(f"https://api.tmdb.org/3/movie/{standardized_id}", params={"api_key": config.tmdb_api_key})
    #     ...
    
    return "en" # Defaulting to English for now as a placeholder

# --- Main Application ---
def main():
    """Main entry point for the CLI video converter."""
    config = Config()
    media_files: list[MediaFile] = []
    
    # Scan media files
    scan_dir: str = config.scan_paths[0] if config.scan_paths else os.getcwd()
    print(f"Scanning directory: {scan_dir}")

    if not os.path.isdir(scan_dir):
        print(f"Error: Scan directory not found at {scan_dir}")
        return

    for filename in os.listdir(scan_dir):
        if filename.endswith(('.mkv', '.mp4', '.avi')):
            file_path: str = os.path.join(scan_dir, filename)
            
            # 1. Parse filename
            standardized_id, media_type = parse_filename(filename)
            
            # 2. Extract stream info
            metadata: dict[str, typing.Any] = extract_stream_info(file_path)
            
            if metadata:
                # 3. Create MediaFile object
                media_file = MediaFile(file_path, standardized_id, media_type, metadata)
                # Fetch original language
                media_file.original_language = get_original_language(config, media_file.standardized_id, media_file.media_type, media_file.metadata)
                media_files.append(media_file)

    if not media_files:
        print("No media files found in the current directory.")
        return

    print(f"Found {len(media_files)} media files:")
    for i, mf in enumerate(media_files):
        print(f"[{i+1}] {mf.standardized_id} ({mf.media_type}) - {os.path.basename(mf.path)}")
        if mf.original_language:
            print(f"    Original Language: {mf.original_language}")

    print("\n--- Conversion Pipeline ---")
    for i, mf in enumerate(media_files):
        print(f"\nFile {i+1}: {os.path.basename(mf.path)}")
        
        # Beautify metadata output
        streams = mf.metadata.get('streams', [])
        print(f"{'Type':<10} | {'Codec':<15} | {'Lang':<5} | {'Details'}")
        print("-" * 60)
        for s in streams:
            details = ""
            if s['codec_type'] == 'video':
                details = f"{s.get('width')}x{s.get('height')} @ {s.get('bit_rate')}"
            elif s['codec_type'] == 'audio':
                details = f"{s.get('channels')} ch"
            elif s['codec_type'] == 'subtitle':
                if s.get('is_forced'):
                    details = "Forced"
                elif s.get('is_sdh'):
                    details = "SDH"
                elif s.get('is_full'):
                    details = "Full"
                else:
                    details = str(s.get('number_frames', 'N/A'))
            
            print(f"{s['codec_type']:<10} | {s['codec_name']:<15} | {s['language']:<5} | {details}")
        
        # --- Intelligent Selection Logic ---
        # Define preferred languages (can be expanded or moved to Config)
        preferred_langs = ['eng', 'en', 'fre', 'fra']
        
        # Determine the "best" language available in the media
        available_langs = {s['language'] for s in streams if s.get('language')}
        
        # Normalize available languages to check against preferred_langs
        # (e.g., if 'fre' is in available_langs, it matches 'fre' in preferred_langs)
        best_lang = next((lang for lang in preferred_langs if lang in available_langs), None)
        
        if not best_lang:
            # If no preferred lang found, just pick the first available language
            best_lang = next(iter(available_langs)) if available_langs else None

        # Define track lists
        audio_tracks = [s for s in streams if s['codec_type'] == 'audio']
        subtitle_tracks = [s for s in streams if s['codec_type'] == 'subtitle']

        # 1. Audio Selection:
        # Priority: Forced -> French (fre/frq) + Original Language -> First Available
        selected_a = []
        if audio_tracks:
            forced_audio = [s for s in audio_tracks if s.get('is_forced')]
            if forced_audio:
                selected_a = [forced_audio[0]['index']]
            else:
                # Identify French tracks
                french_tracks = [s for s in audio_tracks if s.get('language') == 'fre']
                french_canadian = [s for s in audio_tracks if s.get('language') == 'frq']
                
                # Identify Original Language tracks
                orig_lang = mf.original_language if mf.original_language else None
                orig_lang_tracks = [s for s in audio_tracks if s.get('language') == orig_lang] if orig_lang else []

                # Logic:
                # 1. If original language is French, only select French.
                # 2. Otherwise, select French (prefer fre over frq) AND Original Language.
                # 3. If no French, only use Original Language.
                # 4. Fallback to first available.

                if orig_lang == 'fre':
                    # Only French
                    if french_tracks:
                        selected_a = [french_tracks[0]['index']]
                    elif french_canadian:
                        selected_a = [french_canadian[0]['index']]
                    elif orig_lang_tracks:
                        selected_a = [orig_lang_tracks[0]['index']]
                    else:
                        selected_a = [audio_tracks[0]['index']]
                else:
                    # French + Original
                    selected_a = []
                    # Add French if available
                    if french_tracks:
                        selected_a.append(french_tracks[0]['index'])
                    elif french_canadian:
                        selected_a.append(french_canadian[0]['index'])
                    
                    # Add Original Language if available and not already added (if it was French)
                    if orig_lang_tracks and orig_lang_tracks[0]['index'] not in selected_a:
                        selected_a.append(orig_lang_tracks[0]['index'])

                    # If neither French nor Original is available, fallback to first available
                    if not selected_a:
                        selected_a = [audio_tracks[0]['index']]
        
        # 2. Subtitle Selection:
        # Priority: Forced French -> Full French -> French SDH
        selected_s = []
        if subtitle_tracks:
            # Identify French tracks
            french_subs = [s for s in subtitle_tracks if s.get('language') == 'fre']
            
            # 1. Forced French
            forced_french = [s for s in french_subs if s.get('is_forced')]
            if forced_french:
                selected_s.append(forced_french[0]['index'])
            
            # 2. Full French (if not already added as forced)
            full_french = [s for s in french_subs if s.get('is_full') and s['index'] not in selected_s]
            if full_french:
                selected_s.append(full_french[0]['index'])

            # 3. French SDH (if full not available)
            elif not any(s.get('is_full') for s in french_subs):
                sdh_french = [s for s in french_subs if s.get('is_sdh') and s['index'] not in selected_s]
                if sdh_french:
                    selected_s.append(sdh_french[0]['index'])

            # Fallback: If no French subs were found, try to find any subs in the preferred language
            if not selected_s and best_lang:
                lang_subs = [s for s in subtitle_tracks if s.get('language') == best_lang]
                if lang_subs:
                    # Prefer subrip over sup
                    subrip_subs = [s for s in lang_subs if s.get('codec_name') in ['subrip', 'srt']]
                    if subrip_subs:
                        selected_s.append(subrip_subs[0]['index'])
                    else:
                        selected_s.append(lang_subs[0]['index'])
            
            # Final Fallback: If no French or preferred language subs, prefer English subrip, then English sup, then first available
            if not selected_s:
                eng_subs = [s for s in subtitle_tracks if s.get('language') == 'eng']
                if eng_subs:
                    eng_subrip = [s for s in eng_subs if s.get('codec_name') in ['subrip', 'srt']]
                    if eng_subrip:
                        selected_s.append(eng_subrip[0]['index'])
                    else:
                        selected_s.append(eng_subs[0]['index'])
                elif subtitle_tracks:
                    selected_s.append(subtitle_tracks[0]['index'])

            
            # Final Fallback: First available subtitle
            if not selected_s and subtitle_tracks:
                selected_s.append(subtitle_tracks[0]['index'])


        # --- Step 1: Extract Streams ---
        
        # We use ffmpeg to extract individual streams into separate files
        # Naming convention:
        # - Forced: {lang}f.{ext}
        # - SDH: {lang}h.{ext}
        # - Full: {lang}.{ext}
        # - Others: {lang}.{ext}
        
        # Get the directory of the media file
        output_dir = os.path.dirname(mf.path)
        extract_cmd = [config.get_executable("ffmpeg"), "-i", mf.path]
        extracted_files = []

        # Audio tracks
        for a_idx in selected_a:
            a_stream = streams[a_idx]
            lang = a_stream.get('language', 'eng')
            ext = "flac"  # Example: Convert AAC to FLAC for lossless extraction
            
            prefix = ""
            if mf.media_type == "Series":
                # Extract episode number from standardized_id (e.g., "Show_S01E08" -> "08")
                ep_match = re.search(r'E(\d{2})', mf.standardized_id)
                if ep_match:
                    prefix = f"{ep_match.group(1)}-"
            
            out_name = f"{prefix}{lang}.{ext}"
            # Join the output name with the directory of the media file
            extract_cmd.extend(["-map", f"0:{a_idx}", os.path.join(output_dir, out_name)])
            extracted_files.append(os.path.join(output_dir, out_name))

        # Subtitle tracks
        subtitle_counts = {}
        for s_idx in selected_s:
            s_stream = streams[s_idx]
            lang = s_stream.get('language', 'und')
            
            if s_stream.get('is_forced'):
                suffix = "_f"
            elif s_stream.get('is_sdh'):
                suffix = "_h"
            else:
                suffix = ""
            
            prefix = ""
            if mf.media_type == "Series":
                # Extract episode number from standardized_id (e.g., "Show_S01E08" -> "08")
                ep_match = re.search(r'E(\d{2})', mf.standardized_id)
                if ep_match:
                    prefix = f"{ep_match.group(1)}-"
            
            base_name = f"{prefix}{lang}{suffix}"

            if s_stream.get('codec_name') in ['subrip', 'srt']:
                ext = ".srt"
            elif s_stream.get('codec_name') in ['ass', 'ssa']:
                ext = ".ass"
            elif s_stream.get('codec_name') in ['mov_text', 'webvtt']:
                ext = ".vtt"
            elif s_stream.get('codec_name') in ['hdmv_pgs_subtitle', 'dvd_subtitle']:
                ext = ".sup"
            else:
                ext = ".mkv"  # Default to MKV container for unknown subtitle formats
            
            # Handle duplicate names by incrementing
            count = subtitle_counts.get(base_name, 0)
            if count > 0:
                base_name = f"{base_name}{count}"
            subtitle_counts[base_name] = count + 1
            
            out_name = f"{base_name}{ext}"
            # Join the output name with the directory of the media file
            extract_cmd.extend(["-map", f"0:{s_idx}", os.path.join(output_dir, out_name)])
            extracted_files.append(os.path.join(output_dir, out_name))

        print(f"\nStep 1: Extracting selected streams from {os.path.basename(mf.path)}...")
        print(f"Extraction command: {extract_cmd}")
        confirm_step1 = input("Proceed to Step 1 (Extraction)? (y/n): ").lower()
        if confirm_step1 == 'y':
            subprocess.run(extract_cmd, check=True)

        # Convert audio tracks to AAC using eac3to
        audio_files_to_convert = [f for f in extracted_files if f.endswith('.flac')]
        for flac_file in audio_files_to_convert:
            aac_file = flac_file.replace('.flac', '.aac.m4a')
            
            # Determine eac3to arguments based on media type and channel count
            # We need to check the channel count of the original audio stream
            # Since we don't have the stream object easily here, we can check the file or use a default
            # For now, we'll check if it's a series or movie and handle the channel count
            
            eac3to_cmd = [config.get_executable("eac3to"), flac_file, aac_file, "-quality=0.25"]
            
            if mf.media_type == "Series":
                eac3to_cmd.extend(["-downDpl"])
            else:
                # Movie logic: check for more than 6 channels
                # We can use ffprobe to check the channel count of the flac file
                probe_cmd = [
                    config.get_executable("ffprobe"),
                    "-v", "error",
                    "-select_streams", "a",
                    "-show_entries", "stream=channels",
                    "-of", "json",
                    flac_file
                ]
                try:
                    probe_res = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
                    data = json.loads(probe_res.stdout)
                    channels = data.get('streams', [{}])[0].get('channels', 0)
                    if channels > 6:
                        eac3to_cmd.extend(["-down6"])
                except (subprocess.CalledProcessError, json.JSONDecodeError, IndexError, KeyError):
                    pass # Fallback to default if probe fails
            
            print(f"Converting {os.path.basename(flac_file)} to AAC...")
            subprocess.run(eac3to_cmd, check=True)
            # Update the list of files to be muxed to use the new AAC file
            extracted_files = [aac_file if f == flac_file else f for f in extracted_files]
        
        # --- Step 2: Video Encoding ---
        print(f"\nStep 2: Video Encoding...")

        # Take the source media file and encode it based on the media type
        # and the color primaries of the video stream.
        # For simplicity, we'll assume the first video stream is the one we want to encode.
        encode_cmd = [config.get_executable("ffmpeg"), 
                      "-hwaccel", "auto", 
                      "-loglevel", "error", 
                      "-stats", "-i", mf.path]

        video_stream = next((s for s in streams if s['codec_type'] == 'video'), None)
        if video_stream:
            if mf.media_type == "Series":
                # Series: Encode to 720p HEVC
                output_video_path = os.path.join(output_dir, f"{mf.standardized_id}_HEVC-720_Q19-ffmpeg.mkv")
                encode_cmd.extend([
                    "-an", "-sn", 
                    "-vf", "scale=1280:720:flags=spline:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1", 
                    "-c:v", "libx265", 
                    "-x265-params", "vbv-maxrate=6000:vbv-bufsize=6000:deblock=-3,-3:open-gop=0", 
                    "-crf", "19", 
                    "-preset", "slow",
                    output_video_path])
            else:
                color_primaries = video_stream.get('color_primaries', 'bt709')
                if color_primaries == 'bt2020':
                    # HDR movie content
                    output_video_path = os.path.join(output_dir, f"{mf.standardized_id}_2K-HDR_Q19-ffmpeg.mkv")
                    encode_cmd.extend([
                        "-an", "-sn", 
                        "-vf", "scale=1920:1080:flags=spline:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1", 
                        "-c:v", "libx265", 
                        "-profile:v", "main10", 
                        "-pix_fmt", "yuv420p10le", 
                        "-x265-params", "vbv-maxrate=10000:vbv-bufsize=5000:deblock=-3,-3:open-gop=0:early-skip=0:b-intra=0", 
                        "-crf", "19", 
                        "-preset", "medium",
                        output_video_path])
                else:
                    # SDR movie content
                    output_video_path = os.path.join(output_dir, f"{mf.standardized_id}_2K-SDR_Q21-ffmpeg.mkv")
                    encode_cmd.extend([
                        "-an", "-sn", 
                        "-vf", "scale=1920:1080:flags=spline:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1", 
                        "-c:v", "libx265", 
                        "-profile:v", "main10", 
                        "-pix_fmt", "yuv420p10le", 
                        "-x265-params", "vbv-maxrate=10000:vbv-bufsize=5000:deblock=-3,-3:open-gop=0:early-skip=0:b-intra=0", 
                        "-crf", "21", 
                        "-preset", "medium",
                        output_video_path])

        print(f"Encoding command: {encode_cmd}")
        confirm_step2 = input("Proceed to Step 2 (Video encoding )? (y/n): ").lower()
        if confirm_step2 == 'y':
            print(f"Encoding command: {encode_cmd}")
            subprocess.run(encode_cmd, check=True)
            extracted_files.append(output_video_path)

        confirm_step3 = input("Proceed to Step 3 (Final Muxing)? (y/n): ").lower()
        if confirm_step3 == 'y':
            # --- Step 3: Final Muxing ---
            print(f"\nStep 3: Final Muxing...")
                
            # Use the original path as the output for mkvmerge
            mux_cmd = [config.get_executable("mkvmerge"), "-o", f"{mf.path}.mkv"]
            
            # Add the extracted files to the muxing command
            # Note: mkvmerge -o output.mkv file1 file2 ...
            # We need to be careful about the order. Usually, we want the original file as the base if possible, 
            # but since we are extracting, we just mux the extracted files.
            # However, the user's previous logic was to mux the original file with selected tracks.
            # Since we extracted them, we should mux the extracted files into the final mkv.
            
            # To maintain the original file's structure as much as possible, 
            # we'll use the extracted video as the primary source.
            
            # Let's find the extracted video file
            video_extracted = next((f for f in extracted_files if "_video.mkv" in f), None)
            if video_extracted:
                mux_cmd.append(video_extracted)
            
            # Add all other extracted files
            for f in extracted_files:
                if f != video_extracted:
                    mux_cmd.append(f)
            
            subprocess.run(mux_cmd, check=True)
            
            # Cleanup
            for f in extracted_files:
                if os.path.exists(f):
                    os.remove(f)
            print(f"Successfully converted to {mf.path}.mkv")
        else:
            print("Skipping Step 3.")
            # Cleanup extracted files if skipped
            for f in extracted_files:
                if os.path.exists(f):
                    #os.remove(f)
                    pass
            break
            





# --- Utility Functions (To be implemented) ---
def parse_filename(filename: str) -> tuple[str, str]:
    """Parses complex filenames into standardized ID and media type."""
    
    # 1. Determine extension and base name
    name, ext = os.path.splitext(filename)
    ext = ext.lstrip('.') # Remove leading dot
    
    # 2. Series Pattern Check (SXXEXX)
    # Search for SXXEXX pattern in the base name
    series_match: re.Match[str] | None = re.search(r'S(\d{1,2})E(\d{1,2})', name, re.IGNORECASE)
    
    if series_match:
        # Extract the base name part before the series pattern
        start_index = name.find(series_match.group(0))
        if start_index != -1:
            base_name_part = name[:start_index]
        else:
            base_name_part = name
            
        # Standardize the ID format: BaseName_SXXEXX\n
        # Replace dots/spaces with hyphens in the base part, and ensure no trailing/leading separators
        base_name_sanitized = re.sub(r'[.\s]+', '-', base_name_part).strip('-')

        # Capitalize the first letter of each word in the base name part (e.g., "futurama" -> "Futurama")
        base_name_sanitized = '-'.join(word.capitalize() for word in base_name_sanitized.split('-'))
        standardized_id: str = f"{base_name_sanitized}_S{series_match.group(1).zfill(2)}E{series_match.group(2).zfill(2)}"
        return standardized_id, "Series"
    else:
        # 3. Movie Pattern (No SXXEXX)
        # Simple ID: Replace dots and spaces with underscores, and append the extension
        # This aims to match the test expectation of using underscores for separation and including the extension.
        base_name_sanitized = re.sub(r'[.\s]+', '_', name)

        # Clean up any remaining characters that are not alphanumeric or underscore
        base_name_sanitized = re.sub(r'[^\w_]+', '', base_name_sanitized)

        # Extract the year (4 digits) if present, otherwise just use the base name
        year_match = re.search(r'(\d{4})', name)
        if year_match:
            year = year_match.group(1)

            # Remove the year from the base name to avoid duplication
            base_name_sanitized = base_name_sanitized.replace(year, '').strip('_')

            # Reconstruct the ID: BaseName_Year
            standardized_id = f"{base_name_sanitized}_{year}"
        else:
            # Append extension if it exists
            if ext:
                standardized_id = f"{base_name_sanitized}_{ext}"
            else:
                standardized_id = base_name_sanitized

        return standardized_id, "Movie"

def extract_stream_info(file_path: str) -> dict[str, typing.Any]:
    """Uses ffprobe to extract all stream metadata."""
    try:
        # Command to get detailed stream information in JSON format
        command: list[str] = [
            "ffprobe",
            "-v", "error",
            "-show_streams",
            "-of", "json",
            file_path
        ]
        
        # Execute ffprobe
        result: subprocess.CompletedProcess[str] = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        
        # Parse JSON output
        data = json.loads(result.stdout)

        all_streams = []
        for stream in data.get('streams', []):
            stream_info: dict[str, typing.Any] = {
                'codec_type': stream.get('codec_type'),
                'codec_name': stream.get('codec_name'),
                'index': stream.get('index'),
                'language': stream.get('tags', {}).get('language', 'und'),
                'tags': stream.get('tags', {}),
                'codec_tag_language': stream.get('codec_tag_language', 'und'),
                'width': stream.get('width'),
                'height': stream.get('height'),
                'bit_rate': stream.get('bit_rate'),
                'number_frames': stream.get('tags', {}).get('NUMBER_OF_FRAMES'),
                'is_forced': (stream.get('tags', {}).get('forced', '').lower() == 'true' or \
                                   stream.get('tags', {}).get('title', '').lower() == 'forced' or \
                                   stream.get('disposition', {}).get('forced') == 1),
                'is_full': not (stream.get('tags', {}).get('forced', '').lower() == 'true' or \
                                      stream.get('tags', {}).get('title', '').lower() == 'forced' or \
                                      stream.get('disposition', {}).get('forced') == 1 or \
                                      stream.get('tags', {}).get('sdh', '').lower() == 'true' or \
                                      stream.get('tags', {}).get('title', '').lower() == 'sdh' or \
                                      stream.get('disposition', {}).get('hearing_impaired') == 1 or \
                                      stream.get('tags', {}).get('title', '').lower() == 'commentary' or \
                                      stream.get('disposition', {}).get('comment') == 1),
                'is_sdh': (stream.get('tags', {}).get('sdh', '').lower() == 'true' or \
                                  'sdh' in stream.get('tags', {}).get('title', '').lower() or \
                                  stream.get('disposition', {}).get('hearing_impaired') == 1),
                'channels': stream.get('channels', 0)
            }
            all_streams.append(stream_info)
            
        return {"streams": all_streams}

    except subprocess.CalledProcessError as e:
        print(f"Error running ffprobe on {file_path}: {e.stderr}")
        return {}
    except FileNotFoundError:
        print("Error: ffprobe executable not found. Ensure it is in your PATH.")
        return {}
    except json.JSONDecodeError:
        print(f"Error decoding JSON output from ffprobe for {file_path}.")
        return {}

def run_conversion_pipeline(media_file: MediaFile, selected_streams: dict[str, typing.Any]) -> str:
    """
    Orchestrates audio, subtitle, video, and muxing based on selected streams.
    
    Args:
        media_file: The MediaFile object containing metadata and selection state.
        selected_streams: Placeholder for potential future stream selection data (currently unused, relying on media_file state).
        
    Returns:
        The path to the final converted/muxed file.
    """
    if not media_file.selected_video_tracks and not media_file.selected_audio_tracks and not media_file.selected_subtitle_tracks:
        raise ValueError("No streams selected for conversion. Please select at least one stream.")

    # 1. Determine output filename
    base_name: str = media_file.standardized_id
    output_path: str = f"{base_name}_converted.mkv"
    
    print(f"--- Starting Conversion Pipeline for {os.path.basename(media_file.path)} ---")
    print(f"Target Output: {output_path}")
    print(f"Selected Video Tracks: {media_file.selected_video_tracks}")
    print(f"Selected Audio Tracks: {media_file.selected_audio_tracks}")
    print(f"Selected Subtitle Tracks: {media_file.selected_subtitle_tracks}")

    # 2. Build the command list by mapping selected tracks to stream IDs
    
    def _find_stream_id(media_file: MediaFile, track_name: str, stream_type: str) -> int | None:
        """Finds the stream index (ID) matching the track name and type."""
        streams = media_file.metadata.get('streams', [])
        for stream in streams:
            if stream.get('codec_type') == stream_type \
                    and (track_name.lower() in stream.get('codec_name', '').lower() \
                    or track_name.lower() in stream.get('tags', {}).get('language', '').lower()):
                return stream.get('index')
        return None

    video_ids: list[str] = []
    audio_ids: list[str] = []
    subtitle_ids: list[str] = []

    # Map selected video tracks
    for track_name in media_file.selected_video_tracks:
        stream_id: int | None = _find_stream_id(media_file, track_name, 'video')
        if stream_id is not None:
            video_ids.append(str(stream_id))
        else:
            print(f"Warning: Could not find video stream ID for track: {track_name}")

    # Map selected audio tracks
    for track_name in media_file.selected_audio_tracks:
        stream_id: int | None = _find_stream_id(media_file, track_name, 'audio')
        if stream_id is not None:
            audio_ids.append(str(stream_id))
        else:
            print(f"Warning: Could not find audio stream ID for track: {track_name}")

    # Map selected subtitle tracks
    for track_name in media_file.selected_subtitle_tracks:
        stream_id: int | None = _find_stream_id(media_file, track_name, 'subtitle')
        if stream_id is not None:
            subtitle_ids.append(str(stream_id))
        else:
            print(f"Warning: Could not find subtitle stream ID for track: {track_name}")

    # Construct the mkvmerge command
    command: list[str] = [
        "mkvmerge",
        "-o", output_path,
        # -v <video_stream_id> -a <audio_stream_id> -s <subtitle_stream_id> <input_file>
    ]
    
    if video_ids:
        command.extend(["-v", ",".join(video_ids)])
    if audio_ids:
        command.extend(["-a", ",".join(audio_ids)])
    if subtitle_ids:
        command.extend(["-s", ",".join(subtitle_ids)])
        
    command.append(media_file.path)

    print(f"Executing command: {' '.join(command)}")

    print(f"Executing command: {' '.join(command)}")
    
    try:
        # Execute the command
        # NOTE: In a production environment, error handling and stream ID mapping would be critical here.
        subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"Conversion successful! Output saved to {output_path}")
        return output_path
    except subprocess.CalledProcessError as e:
        print(f"Conversion failed with error code {e.returncode}.")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise RuntimeError(f"Conversion pipeline failed: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("mkvmerge executable not found. Ensure it is installed and in your PATH.")

# --- Main Execution Block ---
if __name__ == "__main__":
    main()