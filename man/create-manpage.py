"""
This module configures and provides the command-line argument parser for the 'nominatim_db' CLI
tool.

It adjusts the system path to include the 'src' directory located two levels up from the current
file,
ensuring that the necessary modules can be imported correctly.

Functions:
    get_parser(): Initializes and returns the parser object from 'nominatim_db.cli'.

Usage:
    Call 'get_parser()' to obtain the argument parser for command-line interface interactions.
"""

import sys
from pathlib import Path
from nominatim_db.cli import get_set_parser

sys.path.append(str(Path(__file__, "..", "..", "src").resolve()))


def get_parser():
    """
    Initializes and returns the command-line argument parser.

    This function calls 'get_set_parser()' from the 'nominatim_db.cli' module and returns its
    'parser' attribute.

    Returns:
        argparse.ArgumentParser: The configured command-line argument parser.
    """
    parser = get_set_parser()
    return parser.parser
