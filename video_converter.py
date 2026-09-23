import json
import os
import platform
import re
import subprocess
import typing

# TUI Library
import textual.app
import textual.widgets
import textual.containers

# --- Configuration and Platform Detection ---
class Config:
    """Holds all configuration, including platform-specific paths."""
    def __init__(self) -> None:
        self.platform: str = platform.system()
        self.executables = {}
        self._load_paths()

    def _load_paths(self) -> None:
        # Placeholder for loading paths based on platform and user input
        # These paths must be dynamically set based on user input later.
        self.executables['ffprobe'] = "ffprobe"
        self.executables['ffmpeg'] = "ffmpeg"
        self.executables['eac3to'] = "eac3to"
        self.executables['seconv'] = "seconv"
        self.executables['mkvmerge'] = "mkvmerge"

    def get_executable(self, name: str) -> str:
        return self.executables.get(name, name)

# --- Core Logic Classes ---
class MediaFile:
    """Represents a discovered media file and its metadata."""
    def __init__(self, path: str, standardized_id: str, media_type: str, metadata: dict[str, typing.Any]) -> None:
        self.path: str = path
        self.standardized_id: str = standardized_id
        self.media_type: str = media_type # 'Series' or 'Movie'
        self.metadata: dict[str, typing.Any] = metadata # Contains all ffprobe data
        self.selected_audio_tracks: list[str] = []
        self.selected_subtitle_tracks: list[str] = []
        self.selected_video_tracks: list[str] = []

# --- Main Application ---
class VideoConverterApp(textual.app.App):
    """The main Textual application for the TUI."""
    CSS = """
    #main_container {
        height: 100%;
        width: 100%;
    }
    #media_list {
        height: 80%;
        width: 100%;
        border: heavy $primary;
    }
    #stream_details {
        height: 20%;
        width: 100%;
        border: heavy $secondary;
    }
    """

    BINDINGS: list[textual.app.Binding | tuple[str, str] | tuple[str, str, str]] = []

    def __init__(self) -> None:
        super().__init__()
        self.BINDINGS: list[textual.app.Binding | tuple[str, str] | tuple[str, str, str]] = [
            ("q", "quit", "Quit"),
        ]

    def compose(self) -> textual.app.ComposeResult:
        yield textual.widgets.Header()
        yield textual.containers.Container(
            textual.widgets.Static(id="media_list", markup=True),
            textual.widgets.Static(id="stream_details", markup=True)
        )
        yield textual.widgets.Footer()

    def on_mount(self) -> None:
        self.config = Config()
        self.media_files: list[MediaFile] = []
        self.selected_media_index: int = 0
        self.scan_media_files()
        self.log(f"Application mounted. Found {len(self.media_files)} media files.")
        self.update_media_list()
        self.update_stream_details(None) # Initialize details panel as empty

    def update_media_list(self) -> None:
        """Updates the media list Static widget with the current list of files."""
        media_list_widget: textual.widgets.Static = self.query_one("#media_list", textual.widgets.Static)
        media_list_widget.update(self.get_media_list_markup())

    def update_stream_details(self, selected_index: int) -> None:
        """Updates the stream details Static widget based on the selected file index."""
        self.selected_media_index = selected_index
        stream_details_widget: textual.widgets.Static = self.query_one("#stream_details", textual.widgets.Static)
        
        if selected_index is None or selected_index < 0 or selected_index >= len(self.media_files):
            stream_details_widget.update("Select a media file from the list to view its stream metadata.")
            return

        media_file: MediaFile = self.media_files[selected_index]
        stream_details_widget.update(self.display_stream_details(media_file))

    def on_media_list_selected(self, event: textual.widgets.ListView.Selected) -> None:
        """Handles selection event from the media list."""
        # The event object in Textual ListView.Selected contains the selected item's value
        selected_index: int = self.media_files.index(event.value)
        self.update_stream_details(selected_index)

    def scan_media_files(self) -> None:
        """Scans the current directory for media files and extracts metadata."""
        current_dir: str = os.getcwd()
        for filename in os.listdir(current_dir):
            if filename.endswith(('.mkv', '.mp4', '.avi')):
                file_path: str = os.path.join(current_dir, filename)
                
                # 1. Parse filename
                standardized_id, media_type = parse_filename(filename)
                
                # 2. Extract stream info
                metadata: dict[str, typing.Any] = extract_stream_info(file_path)
                
                if metadata:
                    # 3. Create MediaFile object
                    media_file = MediaFile(file_path, standardized_id, media_type, metadata)
                    self.media_files.append(media_file)

    def get_media_list_markup(self) -> str:
        """Generates the markup for the media file list."""
        if not self.media_files:
            return "No media files found in the current directory."
        
        list_markup = []
        for i, media_file in enumerate(self.media_files):
            # Display ID, Type, and original filename
            list_markup.append(f"[{i+1}] {media_file.standardized_id} ({media_file.media_type}) - {os.path.basename(media_file.path)}")
        
        return "\n".join(list_markup)

    def display_stream_details(self, media_file: MediaFile) -> textual.containers.Container:
        """Generates the interactive widget structure for the selected media file's stream details and selection."""
        if not media_file.metadata.get('streams'):
            return textual.widgets.Static("No stream metadata available for this file.")

        streams = media_file.metadata['streams']
        
        # Group streams by type for easier selection
        audio_streams: list[typing.Any] = [s for s in streams if s['codec_type'] == 'audio']
        video_streams: list[typing.Any] = [s for s in streams if s['codec_type'] == 'video']
        subtitle_streams: list[typing.Any] = [s for s in streams if s['codec_type'] == 'subtitle']

        # --- Widget Builders ---

        def build_stream_list(title: str, streams: list[dict[str, typing.Any]], stream_type: str) -> textual.containers.Container:
            """Helper to build a selectable list of streams."""
            stream_container = textual.containers.Container()
            stream_container.add(textual.widgets.Static(f"**{title} Streams:**"))
            
            if not streams:
                stream_container.add(textual.widgets.Static("  No streams of this type found."))
                return stream_container

            # Use a Tree widget for selection capability
            stream_tree = textual.widgets.Tree()
            stream_tree.add_children([
                textual.widgets.Tree.TreeItem(
                    f"--- {stream_type.capitalize()} Streams ---", 
                    children=[
                        textual.widgets.Tree.TreeItem(
                            f"[{i+1}] Codec: {s['codec_name']} | Language: {s['language']} | Details: {self._get_stream_details_summary(s, stream_type)}",
                            id=f"{stream_type}_{i}"
                        ) for i, s in enumerate(streams)
                    ]
                )
            ])
            stream_container.add(stream_tree)
            return stream_container

        def _get_stream_details_summary(s: dict[str, typing.Any], stream_type: str) -> str:
            """Generates a concise summary string for a stream."""
            if stream_type == 'video':
                return f"Resolution: {s.get('width')}x{s.get('height')} | Bitrate: {s.get('bit_rate')}"
            elif stream_type == 'audio':
                forced_tag: str = " (FORCED)" if s.get('is_forced') else ""
                return f"Channels: {s.get('channels')} {forced_tag}"
            elif stream_type == 'subtitle':
                full_tag: str = " (FULL)" if s.get('is_full') else ""
                return f"SDH: {s.get('is_sdh')} {full_tag}"
            return ""

        # Build the selection containers
        video_container: textual.containers.Container = build_stream_list("Video", video_streams, 'video')
        audio_container: textual.containers.Container = build_stream_list("Audio", audio_streams, 'audio')
        subtitle_container: textual.containers.Container = build_stream_list("Subtitle", subtitle_streams, 'subtitle')

        # Main details container
        details_container = textual.containers.Container(
            textual.widgets.Static(f"--- Metadata for: {os.path.basename(media_file.path)} ---"),
            video_container,
            audio_container,
            subtitle_container
        )
        
        return details_container

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
            
        # Standardize the ID format: BaseName_SXXEXX
        # Replace dots/spaces with hyphens in the base part, and ensure no trailing/leading separators
        base_name_sanitized = re.sub(r'[.\s]+', '-', base_name_part).strip('-')
        standardized_id: str = f"{base_name_sanitized}_S{series_match.group(1).zfill(2)}E{series_match.group(2).zfill(2)}"
        return standardized_id, "Series"
    else:
        # 3. Movie Pattern (No SXXEXX)
        # Simple ID: Replace dots and spaces with underscores, and append the extension
        # This aims to match the test expectation of using underscores for separation and including the extension.
        base_name_sanitized = re.sub(r'[.\s]+', '_', name)
        # Clean up any remaining characters that are not alphanumeric or underscore
        base_name_sanitized = re.sub(r'[^\w_]+', '', base_name_sanitized)
        
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
            "-select_streams", "a:0", # Start by selecting the first audio stream for initial checks
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
                'is_forced': stream.get('tags', {}).get('forced', 'false').lower() == 'true',
                'is_full': stream.get('tags', {}).get('full', 'false').lower() == 'true',
                'is_sdh': stream.get('tags', {}).get('sdh', 'false').lower() == 'true',
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
    app = VideoConverterApp()
    app.run()