"""Configuration for pytest."""

import os


def pytest_report_header():
    """Add environment variables to the pytest report header."""
    return [f"{var}: value" for var in os.environ if var.startswith("TARGET_POSTGRES")]
