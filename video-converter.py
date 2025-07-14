import sys
import os
import subprocess
import random
import re
import multiprocessing
import json

def extract_streams(input_file, output_dir):
    # Use mkvmerge to get track info
    cmd = [
        "mkvmerge", "-J", input_file
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    info = json.loads(result.stdout)
    tracks = info.get("tracks", [])
    if not tracks:
        print(f"No tracks found in {input_file}")
        return [], []
    
    # Now, tracks is a list of dicts similar to MKVFile.tracks
    audio_files = []
    subtitle_files = []

    #debug
    print(f"Found {len(tracks)} tracks in {input_file}")
    print("Track details:")
    print("ID\tType\tCodec\tLanguage\tName")
    for track in tracks:
        print(f"{track['id']}\t{track['type']}\t{track['codec']}\t{track['properties'].get('language', '')}\t{track['properties'].get('track_name', '')}")

    for track in tracks:
        if track["type"] == 'audio':
            # Only extract French audio and original language (if not French)
            lang = track['properties'].get("language", "").lower()
            desc = (track['properties'].get("track_name") or "").lower()
            if "descrip" not in desc:
                # Add language to output filename
                lang_suffix = lang if lang else "eng"
                if "q" in desc and lang.startswith("fr"):
                    lang_suffix = "frq"
                out_audio = os.path.join(
                    output_dir,
                    f"audio_{track['id']}_{lang_suffix}.{track['properties']['codec_id'].split('/')[-1]}"
                )
                cmd = [
                    "mkvextract", "tracks", input_file,
                    f"{track['id']}:{out_audio}"
                ]
                subprocess.run(cmd, check=True)
                audio_files.append(out_audio)
        elif track["type"] == 'subtitles':
            # Only extract French subtitles, skip audio description tracks
            lang = track['properties'].get("language", "").lower()
            desc = (track['properties'].get("track_name") or "").lower()
            forced = track['properties'].get("forced_track", False)
            if lang.startswith("fr"):
                # Identify subtitle type
                if "for" in desc or forced:
                    sub_type = "forced"
                elif "descrip" in desc or "frh" in desc:
                    sub_type = "audio_desc"
                else:
                     sub_type = "full"
                # Determine extension based on codec_id
                codec_id = track.get("codec", "")
                if "SubRip" in codec_id:
                    ext = "srt"
                elif "ASS" in codec_id:
                    ext = "ass"
                elif "HDMV PGS" in codec_id or "VobSub" in codec_id or "PGS" in codec_id or "SUP" in codec_id:
                    ext = "sup"
                else:
                    ext = "sub"
                out_sub = os.path.join(
                    output_dir,
                    f"subtitle_{track['id']}_fr_{sub_type}.{ext}"
                )
                cmd = [
                    "mkvextract", "tracks", input_file,
                    f"{track['id']}:{out_sub}"
                ]
                subprocess.run(cmd, check=True)
                subtitle_files.append(out_sub)

    return audio_files, subtitle_files

def reencode_audio(input_file, audio_files):
    def has_season_episode(filename):
        # Matches S01E01, s01e01, S1E1, etc.
        return re.search(r'[Ss]\d{1,2}[Ee]\d{1,2}', filename) is not None

    audio_cmds = []
    for audio_file in audio_files:
        out_audio = os.path.splitext(audio_file)[0] + "_aac.m4a"
        # Probe channels
        probe_cmd = [
            "ffprobe", "-v", "error", "-select_streams", "a:0",
            "-show_entries", "stream=channels", "-of", "default=noprint_wrappers=1:nokey=1", audio_file
        ]
        result = subprocess.run(probe_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        channels = int(result.stdout.strip()) if result.stdout.strip().isdigit() else 2

        # Determine channel layout
        if has_season_episode(os.path.basename(input_file)):
            channel_opts = "-down2"
        else:
            channel_opts = "-down6" if channels > 6 else ""

        # Use eac3to to encode to HE-AAC (if available)
        eac3to_cmd = [
            "eac3to", audio_file, f"{out_audio}", "-quality=0.25", "5db", channel_opts, "-log=NUL"
        ]
        subprocess.run(eac3to_cmd, check=True)
        audio_cmds.append(out_audio)

    print(f"Converted audio tracks: {audio_cmds}")

def get_video_duration(input_file):
    # Get video duration in seconds using ffprobe
    cmd = [
        "ffprobe", "-v", "error", "-show_entries",
        "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", input_file
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                            text=True)
    return float(result.stdout.strip())

def get_video_bitrate(file_path):
    # Get bitrate in kbps using ffprobe
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=bit_rate", 
        "-of", "default=noprint_wrappers=1:nokey=1", file_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                            text=True)
    bitrate = result.stdout.strip()
    # If bitrate is missing or N/A, calculate from file size and duration
    if bitrate and bitrate.upper() != "N/A":
        return int(bitrate) // 1000  # Convert to kbps
    size_bytes = os.path.getsize(file_path)
    duration = get_video_duration(file_path)
    # Calculate bitrate in kbps (kilobits per second)
    return int((size_bytes * 8) / 1000 / duration)

def reencode_video(input_file, output_file):
    # Limit to 1/3 of CPU threads for x265
    num_threads = max(1, multiprocessing.cpu_count() // 3)
    duration = get_video_duration(input_file)
    if duration <= 120:
        test_start = 0
    else:
        test_start = random.randint(0, int(duration) - 61)
    crf = 21
    min_crf, max_crf = 19, 22
    x265_params = f"vbv-maxrate=6000:vbv-bufsize=6000:early-skip=0:b-intra=0:deblock=-3,-3:pools={num_threads}"
    for _ in range(6):  # Limit to 6 tries
        test_file = f"{output_file}.crf{crf}.test.mkv"
        # Encode 1 minute sample
        cmd = [
            "ffmpeg", "-ss", str(test_start), "-hwaccel", "auto", "-i", input_file,
            "-t", "60", "-map", "0:v:0", "-c:v", "libx265", "-profile:v", "main10", 
            "-pix_fmt",  "yuv420p10le", 
            "-x265-params", x265_params, 
            "-vf", "scale=1920:-8:flags=spline",
            "-preset", "medium", "-crf", str(crf),
            "-an", "-sn", "-y", test_file
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, 
                       stderr=subprocess.DEVNULL)
        bitrate = get_video_bitrate(test_file)
        if bitrate > 2500 and crf < max_crf:
            crf += 1
        elif bitrate > 2500 and crf == max_crf:
            crf = max_crf
            x265_params += ":nr-intra=200:nr-inter=200"
            break  # Stop increasing if at max crf
        elif bitrate < 1500 and crf > min_crf:
            crf -= 1
        else:
            break
    # Encode full file with chosen crf
    cmd = [
        "ffmpeg", "-hwaccel", "auto", "-i", input_file,
        "-map", "0:v:0", "-c:v", "libx265", "-profile:v", "main10", 
        "-pix_fmt",  "yuv420p10le", 
        "-x265-params", x265_params, 
        "-vf", "scale=1920:-8:flags=spline",
        "-preset", "medium", "-crf", str(crf),
        "-an", "-sn", output_file
    ]
    subprocess.run(cmd, check=True)

def remux_to_mkv(video_file, audio_files, subtitle_files, output_file):
    # Prepare mkvmerge command
    cmd = ["mkvmerge", "-o", output_file]

    # Add video
    cmd += ["--track-name", "0:Main Video", video_file]

    # Add audio tracks
    for idx, audio in enumerate(audio_files):
        # Example audio filename: audio_2_frq.aac.m4a or audio_1_eng.aac.m4a
        base = os.path.basename(audio)
        m = re.match(r"audio_\d+_([a-z]{2,3})(?:_([a-z_]+))?", base)
        if m:
            lang = m.group(1)
            sub_type = m.group(2) if m.group(2) else ""
        else:
            lang = "und"
            sub_type = ""
        # Map language codes if needed
        lang_map = {"fr": "fra", "frq": "fra", "eng": "eng"}
        lang_mkv = lang_map.get(lang, lang)
        # Name track
        if sub_type == "forced":
            name = f"{lang_mkv.upper()} Forced"
        elif sub_type == "full":
            name = f"{lang_mkv.upper()} Full"
        elif sub_type:
            name = f"{lang_mkv.upper()} {sub_type.capitalize()}"
        else:
            name = f"{lang_mkv.upper()} AAC {idx+1}"
        cmd += ["--language", f"0:{lang_mkv}", "--track-name", f"0:{name}", audio]

    # Group subtitles by type and language
    sub_tracks = []
    unknown_subs = []
    for sub in subtitle_files:
        m = re.search(r"subtitle_(\d+)_fr_([a-z_]+)\.", os.path.basename(sub))
        if m:
            sub_type = m.group(2)
            if sub_type == "unknown":
                unknown_subs.append(sub)
            else:
                sub_tracks.append((sub_type, sub))
        else:
            sub_tracks.append(("unknown", sub))

    # If there are unknowns, compare their sizes to guess forced/full
    if len(unknown_subs) == 2:
        sizes = [(os.path.getsize(f), f) for f in unknown_subs]
        sizes.sort()
        # Smaller is likely forced, larger is full
        sub_tracks.append(("forced", sizes[0][1]))
        sub_tracks.append(("full", sizes[1][1]))
    else:
        for sub in unknown_subs:
            sub_tracks.append(("unknown", sub))

    # Add subtitle tracks
    for sub_type, sub in sub_tracks:
        lang = "fra"
        if sub_type == "forced":
            name = "French Forced"
            forced_flag = "yes"
        elif sub_type == "full":
            name = "French Full"
            forced_flag = "no"
        elif sub_type == "audio_desc":
            name = "French Audio Description"
            forced_flag = "no"
        else:
            name = "French"
            forced_flag = "no"
        cmd += [
            "--language", f"0:{lang}",
            "--track-name", f"0:{name}",
            "--forced-track", f"0:{forced_flag}",
            sub
        ]

    subprocess.run(cmd, check=True)
    print(f"Remuxed MKV created: {output_file}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python video-converter.py <input.mkv>")
        sys.exit(1)

    input_file = sys.argv[1]
    if not os.path.isfile(input_file):
        print(f"File not found: {input_file}")
        sys.exit(1)

    # Use the same directory as the input file for extraction
    output_dir = os.path.dirname(os.path.abspath(input_file))
    print("Extracting audio and subtitle streams...")
    audio_files, subtitle_files = extract_streams(input_file, output_dir)
    print(f"Extracted audio: {audio_files}")
    print(f"Extracted subtitles: {subtitle_files}")

    # Check if we have any audio or subtitle files to process
    if not audio_files and not subtitle_files:
        print("No audio or subtitle streams found to process.")
    else:
        print(f"Found {len(audio_files)} audio files and {len(subtitle_files)} subtitle files.")
        reencode_audio(input_file, audio_files)
        print("Reencoding audio streams completed.")
    
    # Reencode video stream to h265
    output_video = os.path.splitext(input_file)[0] + "_h265.mkv"
    print("Reencoding video stream to h265...")
    reencode_video(input_file, output_video)
    print(f"Reencoded video saved as: {output_video}")

    # Call the remux function
    remuxed_output = os.path.splitext(input_file)[0] + "_final.mkv"
    remux_to_mkv(
        output_video,
        [os.path.splitext(a)[0] + "_aac.m4a" for a in audio_files],
        subtitle_files,
        remuxed_output
    )
    print(f"Final remuxed file: {remuxed_output}")

if __name__ == "__main__":
    main()
