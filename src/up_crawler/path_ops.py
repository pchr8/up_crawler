from pathlib import Path
from datetime import datetime
from typing import Optional

import tempfile

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

"""
What I want from the function:
    in: directory or file, existing or not
    out: writable thing

    For URIs: I literally need a filename.
    For BS:
        I need a directory where things are put.

    For full:
        I need a directory, where things are put.
"""


def make_path_ok(path: Path | str) -> Path:
    """Return cleaned-up Path"""
    path = Path(path).absolute().resolve()
    return path


def mkdir(path) -> Path:
    """Interpret path as a directory and create it."""
    # TODO there has to be a better way to create a directory in pathlib...
    (path / "x").parent.mkdir(exist_ok=True, parents=True)
    return path


def make_writable(path: Path, is_dir=False) -> Path:
    """Create parents directories if needed."""
    # If it's supposed to be a directory, create it
    if is_dir:
        mkdir(path)
    else:
        # otherwise create all parents so that the file can be written
        path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_file_or_temp(
    path: Optional[Path | str] = None,
    name_or_prefix: Optional[str] = "UPCrawler_tmp",
    fn_if_needed: str = None,
):
    """
    Path can be ex/not-ex file/dir.

    if fn_if_needed is provided, it means we want a file, otherwise it's a dir.
    TODO finish this
    """
    if path:
        if path.is_dir():
            fn_if_needed = (
                fn_if_needed
                if fn_if_needed
                else name_or_prefix + datetime.now().isoformat(sep="T")
            )
            path = path / fn_if_needed
        return make_writable(make_path_ok(path), is_dir=False)
    else:
        return Path(tempfile.mktemp(prefix=name_or_prefix))


def get_dir_or_temp(
    path: Optional[Path | str] = None, name_or_prefix: Optional[str] = "UPCrawler_tmp"
):
    """Make directory OK if it exists otherwise create a temporary one"""
    if path:
        return make_writable(make_path_ok(path), is_dir=True)
    else:
        return Path(tempfile.mkdtemp(prefix=name_or_prefix))
