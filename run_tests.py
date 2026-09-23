import sys
import os
import unittest
from unittest TestLoader, TestCase

# Add the project root to sys.path to allow module imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Dynamically load tests from the tests directory
loader = TestLoader()
# We are loading the test module, which should now be resolvable
suite = loader.loadTestsFromName('tests.test_video_converter')

# Run the tests
runner = unittest.TextTestRunner()
print("--- Running Unit Tests ---")
result = runner.run(suite)

if result.wasSuccessful():
    print("--- All tests passed successfully! ---")
else:
    print("--- Some tests failed or encountered errors. ---")
    sys.exit(1)