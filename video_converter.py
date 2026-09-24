import json
import os
import platform
import re
import subprocess
import typing
import sys
from filelock import FileLock

import requests
from dotenv import load_dotenv

load_dotenv()

LOCK_FILE = os.path.join(os.getcwd(), ".video_converter.lock")
lock = FileLock(LOCK_FILE)

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
    "mkvmerge": "C:\\Program Files\\MKVToolNix\\mkvmerge.exe"
}

LINUX_MEDIA_SCAN_PATHS = [
    "/mnt/d/todo",
]

WINDOWS_MEDIA_SCAN_PATHS = [
    "D:\\todo",
]

# We'll store lookups in a JSON file to avoid redundant API calls.
CACHE_FILE = os.path.join(os.getcwd(), ".tmdb_cache.json")

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
        self.original_language: str | None = None
        self.selected_audio_tracks: list[str] = []
        self.selected_subtitle_tracks: list[str] = []
        self.selected_video_tracks: list[str] = []

# --- Cache Configuration ---

def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}

def save_cache(cache: dict):
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=4)
    except OSError as e:
        print(f"Error saving cache: {e}")

# --- Helper Functions ---
def get_original_language(config: Config, standardized_id: str, media_type: str, metadata: dict[str, typing.Any]) -> str | None:
    """
    Attempts to fetch the original language from TMDB with local caching.
    """
    if not config.tmdb_api_key:
        print("Warning: No TMDB API key configured. Skipping language lookup.")
        return None

    cache = load_cache()
    
    # Check if we already have a result in the cache
    if standardized_id in cache:
        return cache[standardized_id].get("original_language")

    # Fallback logic: if there are 2 audio languages, assume the non-French one is the original.
    audio_streams = [s for s in metadata.get('streams', []) if s.get('codec_type') == 'audio']
    if len(audio_streams) == 2:
        languages = [s.get('language') for s in audio_streams if s.get('language')]
        # Filter out 'fre' and 'fra' (French) and see what's left
        non_french = [lang for lang in languages if lang and lang.lower() not in ['fre', 'fra']]
        if len(non_french) == 1:
            return non_french[0]

    print(f"Attempting to lookup original language for {media_type}: {standardized_id}...")
    
    try:
        # Actual TMDB API call
        endpoint = f"https://api.tmdb.org/3/{media_type.lower()}/{standardized_id}"
        response = requests.get(endpoint, params={"api_key": config.tmdb_api_key}, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            # TMDB usually provides 'original_language' in the response
            original_lang = data.get("original_language")
            
            if original_lang:
                # Update and save cache
                cache[standardized_id] = {
                    "name": data.get("title") or data.get("name"),
                    "original_language": original_lang
                }
                save_cache(cache)
                return original_lang
        else:
            print(f"TMDB API returned status {response.status_code} for {standardized_id}")
            
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to TMDB: {e}")
    
    return "en" # Defaulting to English if lookup fails


# --- Main Application ---
def main():
    """Main entry point for the CLI video converter."""
    
    if lock.is_locked:
        print("Error: Another instance of the video converter is already running.")
        sys.exit(1)

    with lock:
        run_conversion_pipeline()


def run_conversion_pipeline():
    """
    Main conversion pipeline that scans for media files, extracts streams, encodes video, and muxes final output.
    """
    config = Config()

    # Scan media files
    scan_dir: str = config.scan_paths[0] if config.scan_paths else os.getcwd()
    print(f"Scanning directory: {scan_dir}")

    if not os.path.isdir(scan_dir):
        print(f"Error: Scan directory not found at {scan_dir}")
        return

    media_files: list[MediaFile] = []
    for root, dirs, files in os.walk(scan_dir):
        for filename in files:
            if filename.endswith(('.mkv', '.mp4', '.avi')):
                file_path: str = os.path.join(root, filename)
                
                if "ffmpeg" in filename:  
                    # Skip files that are already processed or contain 'ffmpeg' in their name
                    continue

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

    # --- Organization Logic ---
    for mf in media_files:
        parent_dir = os.path.dirname(mf.path)
        # Check if the file is directly in the scan directory (root of scan dir)
        if parent_dir == scan_dir:
            if mf.media_type == "Movie":
                # Movies: Move to folder named after the movie name including the year
                # standardized_id for movies is "BaseName_Year" or "BaseName_Ext"
                # We'll use the standardized_id as the folder name
                new_folder = os.path.join(scan_dir, mf.standardized_id)
                if not os.path.exists(new_folder):
                    os.makedirs(new_folder)
                
                new_path = os.path.join(new_folder, os.path.basename(mf.path))
                os.rename(mf.path, new_path)
                # Update the path in the object
                mf.path = new_path
                print(f"Moved movie to: {new_folder}")
            
            elif mf.media_type == "Series":
                # Series: Group inside a folder named with the series root name excluding episode number but including season
                # standardized_id for series is "BaseName_SXXEXX"
                # We want "BaseName_SXX"
                series_match = re.search(r'(.+)_S(\d{2})E(\d{2})', mf.standardized_id)
                if series_match:
                    base_name = series_match.group(1)
                    season = series_match.group(2)
                    folder_name = f"{base_name}_S{season}"
                    new_folder = os.path.join(scan_dir, folder_name)
                    
                    if not os.path.exists(new_folder):
                        os.makedirs(new_folder)
                    
                    new_path = os.path.join(new_folder, os.path.basename(mf.path))
                    os.rename(mf.path, new_path)
                    # Update the path in the object
                    mf.path = new_path
                    print(f"Moved series to: {new_folder}")

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
        confirm_step1 = input("Proceed to Step 1 (Extraction)? (Y/n): ").lower()
        if confirm_step1 in ('y', ''):
            subprocess.run(extract_cmd, check=True)

        print(f"Step 2: Converting audio tracks to AAC...")
        confirm_step2 = input("Proceed to Step 2 (Audio Conversion)? (Y/n): ").lower()
        if confirm_step2 in ('y', ''):
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
                try:
                    subprocess.run(eac3to_cmd, check=True, capture_output=True, text=True)
                    print(f"Successfully converted {os.path.basename(flac_file)}")
                    extracted_files.append(aac_file)
                except subprocess.CalledProcessError as e:
                    print(f"Error converting {os.path.basename(flac_file)}: {e.stderr}")
                    print(f"STDOUT: {e.stdout}")
                    raise
        else:
            # Audio conversion was skipped, but we still need to ensure that the extracted_files list contains the correct audio files for muxing.
            # We'll filter out the .flac files and add new entries as .aac.m4a files.
            audio_files_to_convert = [f for f in extracted_files if f.endswith('.flac')]
            for flac_file in audio_files_to_convert:
                aac_file = flac_file.replace('.flac', '.aac.m4a')
                extracted_files.append(aac_file)

        
        # --- Step 3: Video Encoding ---
        print("Step 3: Video Encoding...")

        # Take the source media file and encode it based on the media type
        # and the color primaries of the video stream.
        # For simplicity, we'll assume the first video stream is the one we want to encode.
        encode_cmd = [config.get_executable("ffmpeg"), 
                      "-hwaccel", "auto", 
                      "-loglevel", "error", 
                      "-stats", "-i", mf.path]

        video_stream = next((s for s in streams if s.get('codec_type') == 'video'), None)
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
        confirm_step3 = input("Proceed to Step 3 (Video encoding )? (Y/n): ").lower()
        if confirm_step3 in ('y', ''):
            print(f"Encoding command: {encode_cmd}")
            subprocess.run(encode_cmd, check=True)
            extracted_files.append(output_video_path)

        # --- Step 4: Final Muxing ---
        print("Step 4: Final Muxing...")
        
        # Determine the format string (e.g., HEVC-720) - this would ideally be parsed from the encoded video file name
        # For this implementation, we'll assume a placeholder or logic to extract it.
            
        if mf.media_type == "Series":
            # Series: Encode to 720p HEVC
            video_format = "HEVC-720"
        else:
            color_primaries = video_stream.get('color_primaries', 'bt709')
            if color_primaries == 'bt2020':
                video_format = "2K-HDR"
            else:
                video_format = "2K-SDR"
        
        # Build the output filename: {standardized_id}_[{video_format}_{lang1}-{lang2}].mkv
        # Example: Futurama_S11E08_[HEVC-720_FRE-ENG].mkv
        # We need to collect the language codes for the tracks being included.
        langs_to_include = []
        for a_idx in selected_a:
            a_stream = streams[a_idx]
            langs_to_include.append(a_stream.get('language', 'und'))
        for s_idx in selected_s:
            s_stream = streams[s_idx]
            langs_to_include.append(s_stream.get('language', 'und'))
        
        # Unique and sorted language codes for the filename (French first)
        unique_langs = sorted(set(langs_to_include), key=lambda x: (x.lower() not in ['fre', 'fra'], x))
        lang_str = "-".join(unique_langs)
        output_filename = f"{media_file.standardized_id}_[{video_format}_{lang_str.upper()}].mkv"
        output_path = os.path.join(os.path.dirname(mf.path), output_filename).replace("__", "_")


        # Construct the mkvmerge command
        # Example: mkvmerge --output ... --no-track-tags --no-global-tags --language 0:und ... (input_file) ...
        command = [
            config.get_executable("mkvmerge"),
            "-o", output_path,
            "--no-track-tags",
            "--no-global-tags",
            "--color-matrix-coefficients", "0:1",
            "--color-range", "0:1",
            "--color-transfer-characteristics", "0:1",
            "--color-primaries", "0:1",
            "--compression", "0:none",
            f"{output_video_path}"
        ]

        # Add the extracted files to the command with their specific parameters
        # We iterate through the extracted_files which contains the paths to the audio and subtitle files
        french_audio_found = False

        # 1. French Audio
        for file_path in extracted_files:
            # Check for French codes (fr, fra, frq) and the correct extension
            if any(code in file_path for code in ["fr", "fra", "frq"]) and file_path.endswith('.aac.m4a'):
                if "frq" in file_path:
                    command.extend(["--no-global-tags", "--no-chapters", 
                                    "--language", "0:fr", 
                                    "--track-name", "0:Quebecquois", 
                                    "--compression", "0:none", f"{file_path}"])
                else:
                    command.extend(["--no-global-tags", "--no-chapters", 
                                    "--language", "0:fr", 
                                    "--compression", "0:none", f"{file_path}"])
                french_audio_found = True

        # 2. Original/Other Audio
        for file_path in extracted_files:
            # Catch all other .aac.m4a files that weren't handled by the French logic
            if file_path.endswith('.aac.m4a') and not any(code in file_path for code in ["fr", "fra", "frq"]):
                lang_match = re.search(r'([a-z]{3})\.aac\.m4a$', file_path)
                lang = lang_match.group(1) if lang_match else "en"
                command.extend(["--no-global-tags", "--no-chapters", 
                                "--language", f"0:{lang}", 
                                "--compression", "0:none", f"{file_path}"])

        # 3. Subtitles
        for file_path in extracted_files:
            if file_path.endswith(('.srt', '.ass')):
                if "fre_f" in file_path:
                    command.extend(["--language", "0:fr", 
                                    "--track-name", "0:Forced", 
                                    "--forced-display-flag", "0:yes", 
                                    "--compression", "0:none", f"{file_path}"])
                elif "fre" in file_path:
                    command.extend(["--language", "0:fr", 
                                    "--default-track-flag", "0:no" if french_audio_found else "0:yes",
                                    "--track-name", "0:Complet", 
                                    "--compression", "0:none", f"{file_path}"])
                else:
                    command.extend(["--language", "0:en", 
                                    "--compression", "0:none", f"{file_path}"])

        print(f"Final Muxing command: {' '.join(command)}")
        confirm_step4 = input("Proceed to Step 4 (Final Muxing)? (Y/n): ").lower()
        if confirm_step4 in ('y', ''):
            try:
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


# --- Main Execution Block ---
if __name__ == "__main__":
    main()