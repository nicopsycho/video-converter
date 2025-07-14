import sys
import os
import subprocess
import random
import re
import multiprocessing

def extract_streams(input_file, output_dir):
    # Use mkvmerge to get track info
    cmd = [
        "mkvmerge", "-i", input_file
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    tracks = []
    for line in result.stdout.splitlines():
        # Example: Track ID 0: video (HEVC)
        m = re.match(r"Track ID (\d+): (\w+) \(([^)]+)\)(?: \((.+)\))?", line)
        if m:
            track_id = int(m.group(1))
            track_type = m.group(2)
            codec_id = m.group(3)
            # mkvmerge -i does not provide language or name, so we get them with mkvmerge --identify-verbose
            tracks.append({"track_id": track_id, "track_type": track_type, "codec_id": codec_id})

    # Get language and track name using mkvmerge --identify-verbose
    cmd = [
        "mkvmerge", "--identify-verbose", input_file
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    lang_map = {}
    name_map = {}
    for line in result.stdout.splitlines():
        # Example: Track ID 1: audio (A_AAC) [language:fra] [name:French]
        m = re.match(r"Track ID (\d+): (\w+) \([^)]+\)(?: \[language:([^\]]+)\])?(?: \[name:([^\]]+)\])?", line)
        if m:
            tid = int(m.group(1))
            lang = m.group(3)
            name = m.group(4)
            if lang:
                lang_map[tid] = lang
            if name:
                name_map[tid] = name

    # Attach language and name to tracks
    for t in tracks:
        t["language"] = lang_map.get(t["track_id"], "")
        t["track_name"] = name_map.get(t["track_id"], "")

    # Now, tracks is a list of dicts similar to MKVFile.tracks
    audio_files = []
    subtitle_files = []

    for track in tracks:
        if track["track_type"] == 'audio':
            out_audio = os.path.join(output_dir, f"audio_{track['track_id']}.{track['codec_id'].split('/')[-1]}")
            cmd = [
                "mkvextract", "tracks", input_file,
                f"{track['track_id']}:{out_audio}"
            ]
            subprocess.run(cmd, check=True)
            # Only extract French audio and subtitles
            if hasattr(track, "language") and track["language"] and track["language"].lower().startswith("fr"):
                audio_files.append(out_audio)
        elif track["track_type"] == 'subtitles':
            # Only extract French subtitles
            if hasattr(track, "language") and track["language"] and track["language"].lower().startswith("fr"):
                # Identify subtitle type
                desc = (track["track_name"] or "").lower() if hasattr(track, "track_name") and track["track_name"] else ""
                if "for" in desc:
                    sub_type = "forced"
                elif "full" in desc or "complet" in desc:
                    sub_type = "full"
                elif "audio" in desc or "description" in desc \
                    or "audiodescription" in desc \
                    or "frh" in desc \
                        or "audio desc" in desc:
                    sub_type = "audio_desc"
                else:
                    sub_type = "unknown"
                # Determine extension based on codec_id
                if "SubRip" in track.codec_id:
                    ext = "srt"
                elif "ASS" in track.codec_id:
                    ext = "ass"
                elif "HDMV PGS" in track.codec_id \
                    or "VobSub" in track.codec_id \
                    or "PGS" in track.codec_id \
                        or "SUP" in track.codec_id:
                    ext = "sup"
                else:
                    ext = "sub"
                out_sub = os.path.join(output_dir, f"subtitle_{track.track_id}_fr_{sub_type}.{ext}")
                cmd = [
                    "mkvextract", "tracks", input_file,
                    f"{track.track_id}:{out_sub}"
                ]
                subprocess.run(cmd, check=True)
                subtitle_files.append(out_sub)

            # Determine extension based on codec_id
            if "SubRip" in track.codec_id:
                ext = "srt"
            elif "ASS" in track.codec_id:
                ext = "ass"
            elif "HDMV PGS" in track.codec_id or "VobSub" in track.codec_id or "PGS" in track.codec_id or "SUP" in track.codec_id:
                ext = "sup"
            else:
                ext = "sub"
            out_sub = os.path.join(output_dir, f"subtitle_{track.track_id}.{ext}")
            cmd = [
                "mkvextract", "tracks", input_file,
                f"{track.track_id}:{out_sub}"
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
            channel_opts = ["-ac", "2"]
        else:
            channel_opts = ["-ac", "6"] if channels > 6 else ["-ac", str(channels)]

        # Analyze max volume
        vol_cmd = [
            "ffmpeg", "-i", audio_file, "-af", "volumedetect", "-vn", "-sn", "-dn",
            "-f", "null", "-"
        ]
        vol_proc = subprocess.run(vol_cmd, stderr=subprocess.PIPE, text=True)
        max_vol = -1.0
        for line in vol_proc.stderr.splitlines():
            if "max_volume:" in line:
                try:
                    max_vol = float(line.split("max_volume:")[1].split(" dB")[0].strip())
                except Exception:
                    pass

        # Try to increase gain by 6dB, but not above 0dB
        gain = 6.0
        if max_vol + gain > 0:
            gain = -max_vol  # so max is 0dB

        ffmpeg_cmd = [
            "ffmpeg", "-i", audio_file, *channel_opts,
            "-c:a", "libfdk_aac", "-profile:a", "aac_he_v2",
            "-af", f"volume={gain}dB",
            "-y", out_audio
        ]
        subprocess.run(ffmpeg_cmd, check=True)
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
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=bit_rate", 
        "-of", "default=noprint_wrappers=1:nokey=1", file_path
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                            text=True)
    bitrate = result.stdout.strip()
    if bitrate:
        return int(bitrate) // 1000  # Convert to kbps
    # fallback: calculate bitrate from file size and duration
    size_kb = os.path.getsize(file_path) // 1024
    duration = get_video_duration(file_path)
    return int(size_kb * 8 / duration)  # kbps

def reencode_video(input_file, output_file):
    # Limit to 1/3 of CPU threads for x265
    num_threads = max(1, multiprocessing.cpu_count() // 3)
    duration = get_video_duration(input_file)
    if duration <= 120:
        test_start = 0
    else:
        test_start = random.randint(0, int(duration) - 61)
    test_file = output_file + ".test.mkv"
    crf = 21
    min_crf, max_crf = 19, 22
    x265_params = f"vbv-maxrate=6000:vbv-bufsize=6000:early-skip=0:b-intra=0:deblock=-3,-3:pools={num_threads}"
    for _ in range(6):  # Limit to 6 tries
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
    os.remove(test_file)
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
        lang = "fra"
        name = f"French AAC {idx+1}"
        cmd += ["--language", f"0:{lang}", "--track-name", f"0:{name}", audio]

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

    output_video = os.path.splitext(input_file)[0] + "_h265.mkv"
    print("Reencoding video stream to h265...")
    reencode_video(input_file, output_video)
    print(f"Reencoded video saved as: {output_video}")

    reencode_audio(input_file, audio_files)
    print("Reencoding audio streams completed.")

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
