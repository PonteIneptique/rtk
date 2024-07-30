import glob
import shutil
import os.path
from typing import List
from requests_mock import Mocker

_HERE = os.path.relpath(os.path.dirname(__file__))


def get_env_bin(env, binary):
    """

    :param env:
    :param binary:
    :return:

    >>> get_env_bin("yaltaienv", "yaltai")
    'yaltaienv/bin/yaltai'
    """
    return os.path.normpath(os.path.relpath(os.path.join(_HERE, "..", env, "bin", binary)))


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


def register_uri(mocker: Mocker, uri: str, file: str, method: str = "GET", binary: bool = False) -> None:
    """ Adds the content of a file to mock a URI response

    :param mocker: Mocker session
    :param uri: URI to mock
    :param file: File to parse content from
    :param method: HTTP Method
    """
    read_mode = "r" if not binary else "rb"
    with open(get_input(file)[0], read_mode) as f:
        if binary:
            mocker.register_uri(method, uri, content=f.read())
        else:
            mocker.register_uri(method, uri, text=f.read())


def copy_input(input_string: str) -> List[str]:
    """

    :param input_string:
    :return:

    >>> copy_input("manifest1.json")
    ['test_output/manifest1.json']
    """
    os.makedirs(get_output(""), exist_ok=True)
    out = []
    for file in get_input(input_string):
        out.append(os.path.abspath(get_output(os.path.basename(file))))
        os.symlink(os.path.abspath(file), out[-1])
    return [
        os.path.relpath(file)
        for file in out
    ]
