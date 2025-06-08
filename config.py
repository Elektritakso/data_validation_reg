"""
Configuration settings for the application.
"""
import os
import tempfile

# Feature flags
ENABLE_REGULATIONS = True

# Application settings
DEBUG = True
MAX_CONTENT_LENGTH = 2048 * 1024 * 1024  # 2GB
TEMP_FOLDER = os.path.join(tempfile.gettempdir(), 'csv_validator')
