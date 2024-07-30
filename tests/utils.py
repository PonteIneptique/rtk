import os.path
import glob
from typing import List
import shutil
import pytest

_HERE = os.path.relpath(os.path.dirname(__file__))


def get_input(path_like: str) -> List[str]:
    """ Return a list of content matching `path_like`

    :param path_like: A UNIX wildcard path or normal path
    :return: List of matching files or directories

    >>> get_input("lorem.pdf")
    ['tests/assets/lorem.pdf']
    """
    return glob.glob(os.path.join(_HERE, "assets", path_like))


def get_output(path_like: str) -> str:
    """ Return the output path for a given string

    :param path_like: A UNIX wildcard path or normal path
    :return: List of matching files or directories

    >>> get_output("lorem.pdf")
    'test_output/lorem.pdf'
    """
    return os.path.relpath(os.path.join(_HERE, "..", "test_output", path_like))


def clear_output():
    shutil.rmtree(get_output(""))


@pytest.fixture
def remove_output():
    if os.path.exists(get_output("")):
        clear_output()
    yield
    if os.path.exists(get_output("")):
        clear_output()
