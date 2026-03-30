"""
Root conftest.py — sets dummy environment variables before any test module
is imported. Required because config.settings.Settings() is instantiated at
module level and raises ValidationError if required keys are absent.
"""
import os

os.environ.setdefault("GEMINI_API_KEY", "fake-key-for-ci-testing")
os.environ.setdefault("YOUTUBE_API_KEY", "fake-key-for-ci-testing")
os.environ.setdefault("BALLDONTLIE_API_KEY", "fake-key-for-ci-testing")
os.environ.setdefault("NEO4J_PASSWORD", "fake-password-for-ci-testing")
