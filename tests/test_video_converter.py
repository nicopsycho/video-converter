import sys

# Add the project root to sys.path to allow module imports
sys.path.insert(0, r"g:\GIT\video-converter")

import unittest
from unittest.mock import patch
import json
import subprocess
from video_converter import parse_filename, extract_stream_info

class TestFilenameParsing(unittest.TestCase):
    """Tests for parse_filename function."""

    def test_parse_filename_series(self):
        """Tests parsing of a typical series filename."""
        filename = "The.Show.S02E03.MULTi.2160p.WEB.H265-AMB3R.mkv"
        expected_id = "The-Show_S02E03"
        expected_type = "Series"
        self.assertEqual(parse_filename(filename), (expected_id, expected_type))

    def test_parse_filename_movie(self):
        """Tests parsing of a typical movie filename."""
        filename = "Inception.2160p.WEB.H265.mkv"
        # Note: The current implementation uses a simple heuristic for movies
        expected_id = "Inception_2160p_WEB_H265_mkv"
        expected_type = "Movie"
        self.assertEqual(parse_filename(filename), (expected_id, expected_type))

    def test_parse_filename_unknown(self):
        """Tests parsing of a filename that doesn't match known patterns."""
        filename = "unknown_file.mp4"
        expected_id = "unknown_file_mp4"
        expected_type = "Movie" # Falls back to Movie based on current logic
        self.assertEqual(parse_filename(filename), (expected_id, expected_type))

    def test_parse_filename_complex_series(self):
        """Tests parsing of a complex series filename with multiple tags."""
        filename = "Show.Name.S01E01.HDRip.720p.WEB-GROUP.mkv"
        expected_id = "Show-Name_S01E01"
        expected_type = "Series"
        self.assertEqual(parse_filename(filename), (expected_id, expected_type))

    def test_parse_filename_simple_movie(self):
        """Tests parsing of a very simple movie filename."""
        filename = "MyMovie.mkv"
        expected_id = "MyMovie_mkv"
        expected_type = "Movie"
        self.assertEqual(parse_filename(filename), (expected_id, expected_type))

    def test_parse_filename_minimal_movie(self):
        """Tests parsing of a minimal movie filename."""
        filename = "MovieTitle.mp4"
        expected_id = "MovieTitle_mp4"
        expected_type = "Movie"
        self.assertEqual(parse_filename(filename), (expected_id, expected_type))

    @patch('subprocess.run')
    def test_extract_stream_info_success(self, mock_subprocess_run):
        """Tests that extract_stream_info correctly parses successful ffprobe output."""
        file_path = "test_media.mkv"
        # Configure the mock to return a successful structure
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=['ffprobe', '-v', 'error', '-select_streams', 'a:0', '-show_streams', '-of', 'json', file_path],
            returncode=0,
            stdout=json.dumps({
                'streams': [
                    {'codec_type': 'video', 'codec_name': 'h264', 'width': 1920, 'height': 1080, 'tags': {}},
                    {'codec_type': 'audio', 'codec_name': 'aac', 'tags': {'forced': 'true'}}
                ]
            }),
            stderr=''
        )
        stream_info = extract_stream_info(file_path)
        self.assertIsNotNone(stream_info)
        self.assertIn('streams', stream_info)
        self.assertEqual(stream_info['streams'][0]['codec_name'], 'h264')

    @patch('subprocess.run', side_effect=subprocess.CalledProcessError(1, "ffprobe", stderr="Error"))
    def test_extract_stream_info_failure(self, mock_subprocess_run):
        """Tests that extract_stream_info handles ffprobe failure gracefully."""
        file_path = "bad_file.mkv"
        stream_info = extract_stream_info(file_path)
        self.assertEqual(stream_info, {})

    @patch('subprocess.run', side_effect=FileNotFoundError)
    def test_extract_stream_info_file_not_found(self, mock_subprocess_run):
        """Tests that extract_stream_info handles FileNotFoundError."""
        result = extract_stream_info("non_existent_file.mkv")
