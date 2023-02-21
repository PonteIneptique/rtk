# Std lib
from typing import Tuple, List, Optional, Dict, Union
import os
import hashlib
import csv
from pathlib import Path
# Non std lib
import pyvips
import requests
import lxml.etree as ET


def download(url: str, target: str, options: Optional[Dict[str, str]] = None) -> Optional[str]:
    """ Download the element at [URL] and saves it at [TARGET] using binary writing. [OPTIONS] are fed to the headers

    :param url: A url
    :param target: A destination path
    :param options: A key-value dict for the request headers
    :return: The path where the file was saved or None if the download failed.
    """
    headers = {}
    headers.update(options or {})
    os.makedirs(os.path.dirname(target), exist_ok=True)
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        with open(target, 'wb') as handle:
            handle.write(response.content)
        return target
    except Exception as E:
        print(E)
        return None


def download_iiif_image(url: str, target: str, options: Optional[Dict[str, Union[str, int]]] = None) -> str:
    """ Download the IIIF image at [URL] and saves it at [TARGET] using binary writing. [OPTIONS] are mostly fed to the
        headers except for `max_width` and `max_height` keys which are used for limiting the image size (instead of
        full size image). You cannot use max_width and max_height at the same time.

    :param url: A url
    :param target: A destination path
    :param options: A key-value dict for the request headers
    :return: The path where the file was saved or None if the download failed.
    """
    if options.get("max_height"):
        url = url.replace("/full/full/", f"/full/,{options['max_height']}/")
    elif options.get("max_width"):
        url = url.replace("/full/full/", f"/full/{options['max_width']},/")
    return download(
        url,
        target,
        {
            key: val
            for key, val in options.items()
            if key not in {"max_width", "max_height"}
        }
    )


def download_iiif_manifest(url: str, target: str, options: Optional[Dict[str, str]] = None) -> Optional[str]:
    """ Download the element at [URL] and saves it at [TARGET] using plain-text writing. [OPTIONS] are fed to
        the headers. In case of failure, print the exception and return None. The manifest is read and the data is
        compiled as a CSV

    :param url: A url
    :param target: A destination path
    :param options: A key-value dict for the request headers
    :return: The path where the file was saved or None if the download failed.
    """
    os.makedirs(os.path.dirname(target), exist_ok=True)
    headers = {}
    headers.update(options or {})
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        j = response.json()
    except Exception as E:
        print(E)
        return None
    rows = []
    dirname = os.path.splitext(os.path.basename(target))[0]
    if "items" in j:
        for element in j["items"]:
            rows.append([element["items"][0]["items"][0]["body"]["id"], dirname])
    elif "sequences" in j:
        for element in j["sequences"][0]["canvases"]:
            rows.append([element["images"][0]["resource"]["@id"], dirname])

    with open(target, 'w') as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)

    return url


def check_content(filepath, ratio: Union[int, float] = 1):
    """ Check that [FILEPATH] XML ALTO has at least [RATIO] content done. If [RATIO] is an int (ratio=2), check that it
        has at least [RATIO] lines (Here, >= 2). If [RATIO] is a float, check that it has
        `SUM(LINE WITH TEXT)/COUNT(LINES)` >= [RATIO]

    :param filepath: ALTO file to check
    :param ratio: Float (Percent) or Int (Absolute) threshold
    :return: True if the file has above N lines, False if it needs to be OCRized
    """
    try:
        xml = ET.parse(filepath)
    except Exception:
        return False
    data = []
    for content in xml.xpath("//a:String/@CONTENT", namespaces={"a": "http://www.loc.gov/standards/alto/ns-v4#"}):
        data.append(int(bool(str(content))))
    if len(data) == 0:  # The document has no lines
        return True
    elif isinstance(ratio, int):
        return sum(data) >= ratio
    elif isinstance(ratio, float):
        return (sum(data) / (len(data) or 1)) >= ratio
    return False


def clean_kraken_filename(filepath: str) -> Optional[str]:
    """ Kraken writes a relative path to image in its XML serialization using the Current Working Directory. This
    function makes it relative to the file.

    :param filepath: File to correct
    :returns: Name of the fixed file. None if it failed.
    """
    try:
        xml = ET.parse(filepath)
        for content in xml.xpath("//a:fileName", namespaces={"a": "http://www.loc.gov/standards/alto/ns-v4#"}):
            content.text = os.path.basename(content.text)
    except Exception:
        return None

    with open(filepath, "w") as f:
        f.write(ET.tostring(xml, encoding=str))

    return filepath


def check_kraken_filename(filepath: str) -> bool:
    """ Kraken writes a relative path to image in its XML serialization using the Current Working Directory. This
    checks whether it was corrected.

    :param filepath: File to check
    :returns: Boolean indicator of the check
    """
    try:
        xml = ET.parse(filepath)
        for content in xml.xpath("//a:fileName", namespaces={"a": "http://www.loc.gov/standards/alto/ns-v4#"}):
            if "/" in content.text:
                return False
    except Exception:
        return False
    return True


def batchify_textfile(filepath: str, batch_size: int = 100):
    """ Reads a list of file to process and batch them

    :param filepath:
    :param batch_size:
    :return:

    """
    with open(filepath) as f:
        text = f.read().split()
    return [text[n:n+batch_size] for n in range(0, len(text), batch_size)]


def change_ext(filepath: str, new_ext: str) -> str:
    return os.path.splitext(filepath)[0] + f".{new_ext}"


def get_name_before_manifest_json(url):
    return url.split("/")[-2]


def string_to_hash(url: str) -> str:
    result = hashlib.sha256(url.encode())
    return result.digest().decode()


def alto_zone_extraction(filepath: str, zones: List[str]):
    """ Retrieves only given zone types in filepath

    :param filepath:
    :param zones:
    :return: Agglutinated Lines

    >>> alto_zone_extraction("../test_dir/AEV_3090_1870_Goms_Ausserbinn_010.xml", ["Col"])
    """
    try:
        xml = ET.parse(filepath)
    except Exception:
        return False
    ns = dict(namespaces={"a": "http://www.loc.gov/standards/alto/ns-v4#"})
    # <OtherTag ID="TYPE_35" LABEL="Adresse"/>
    allowed_tags = [
        str(otherTag.attrib["ID"])
        for otherTag in xml.xpath("//a:OtherTag", **ns)
        if str(otherTag.attrib["LABEL"]) in zones
    ]
    out_text = []
    for zone in xml.xpath("//a:TextBlock", **ns):
        if str(zone.attrib.get("TAGREFS")) in allowed_tags:
            for line in zone.xpath(".//a:TextLine", **ns):
                out_text.append(" ".join([string for string in line.xpath("./a:String/@CONTENT", **ns)]))

    return "\n".join(out_text)


def pdf_extract(pdf_path: str, start_on: int = 2):
    """ Given a PDF file, generates a new folder with all extracted images

    Code adapted from Kraken 4.3.1

    :param pdf_path:
    :param start_on: Page to start on (Default is 2 because Gallica adds generated metadata)
    :return:
    """
    n_pages = pdf_get_nb_pages(pdf_path)
    scheme = pdf_name_scheme(pdf_path)
    os.makedirs(Path(scheme).parent, exist_ok=True)
    scheme = str(scheme)
    out = []
    for i in range(start_on, n_pages):
        doc = pyvips.Image.new_from_file(pdf_path, dpi=300, page=i, access="sequential")
        local_targ = scheme.format(i)
        doc.write_to_file(local_targ)
        out.append(str(local_targ))
    return out


def pdf_name_scheme(pdf_path: str) -> str:
    path = Path(pdf_path)
    target = Path(os.path.join(path.parent, path.stem))
    os.makedirs(target, exist_ok=True)
    return str(Path.joinpath(target, "f{}.jpg"))


def pdf_get_nb_pages(pdf_path: str) -> int:
    doc = pyvips.Image.new_from_file(pdf_path, dpi=300, n=-1, access="sequential")
    if 'n-pages' not in doc.get_fields():
        raise Exception("No page count in the PDF")
    return doc.get('n-pages')


def simple_args_kwargs_wrapper(function, **kwargs):
    """ Wrap a function with additional kwargs. Convert tuple input as multiple args.

    >>> wrapped_sum = simple_args_kwargs_wrapper(sum)
    >>> wrapped_sum(([2, 3, 4]))
    9
    >>> wrapped_list = simple_args_kwargs_wrapper(list)
    >>> wrapped_list("abc")
    ['a', 'b', 'c']
    >>> wrapped_sort = simple_args_kwargs_wrapper(sorted, key=lambda x: -x)
    >>> wrapped_sort([1, 2, 3])
    [3, 2, 1]
    >>> wrapped_sort = simple_args_kwargs_wrapper(sorted, key=lambda x: -x)
    >>> wrapped_sort([1, 2, 3])
    [3, 2, 1]
    """
    def wrapped(args):
        if isinstance(args, tuple):
            return function(*args, **kwargs)
        return function(args, **kwargs)
    return wrapped
