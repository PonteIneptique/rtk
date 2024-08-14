# Std lib
from typing import Tuple, List, Optional, Dict, Union, Any, Callable
import os
import hashlib
import csv
from pathlib import Path
# Non std lib
import fitz  # PyMuPDF
import requests
import lxml.etree as ET
import cases
import unidecode
from xml.sax.saxutils import escape



def split_batches(inputs: List[str], splits: int) -> List[List[str]]:
    """ Split a number of inputs into N splits, more or less even ones"""
    if splits <= 0:
        raise ValueError("Number of splits must be greater than zero.")
    
    # Calculate the base size of each split and the number of splits that need an extra element
    base_size = len(inputs) // splits
    extra_elements = len(inputs) % splits
    
    result = []
    start = 0
    
    for i in range(splits):
        end = start + base_size + (1 if i < extra_elements else 0)
        result.append(inputs[start:end])
        start = end
    
    return result



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
    dirname = clean_kebab(j["label"])
    if "items" in j:
        for idx, element in enumerate(j["items"]):
            rows.append([element["items"][0]["items"][0]["body"]["id"], dirname, f"f{idx}-"+clean_kebab(element["label"])])
    elif "sequences" in j:
        for idx, canvas in enumerate(j["sequences"][0]["canvases"]):
            elm = cleverer_manifest_parsing(canvas["images"][0])
            if elm:
                rows.append([elm, dirname, f"f{idx}-"+clean_kebab(canvas["label"])])

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
    result = hashlib.sha256(url.encode("utf-8"))
    return result.hexdigest()[:10]


def alto_zone_extraction(
        filepath: str,
        zones: Optional[List[str]] = None
) -> Optional[Dict[str, Union[str, List[str]]]]:
    """ Retrieves only given zone types in filepath

    :param filepath:
    :param zones:
    :return: Agglutinated Lines

    """
    zones = zones or []
    try:
        xml = ET.parse(filepath)
    except Exception:
        return None
    ns = dict(namespaces={"a": "http://www.loc.gov/standards/alto/ns-v4#"})
    # <OtherTag ID="TYPE_35" LABEL="Adresse"/>
    allowed_tags = []
    if zones:
        allowed_tags = [
            str(otherTag.attrib["ID"])
            for otherTag in xml.xpath("//a:OtherTag", **ns)
            if str(otherTag.attrib["LABEL"]) in zones
        ]
    label_map = {
        str(otherTag.attrib["ID"]): str(otherTag.attrib["LABEL"])
        for otherTag in xml.xpath("//a:OtherTag", **ns)
    }

    out_text = []
    for zone in xml.xpath("//a:TextBlock", **ns):
        if zones == [] or str(zone.attrib.get("TAGREFS")) in allowed_tags:
            out_text.append({
                "type": label_map.get(str(zone.attrib.get("TAGREFS")), "Undispatched"),
                "lines": []
            })
            for line in zone.xpath(".//a:TextLine", **ns):
                out_text[-1]["lines"].append(
                    " ".join(
                        [
                            str(string)
                            for string in line.xpath("./a:String/@CONTENT", **ns)
                        ]
                    )
                )
    return out_text


def pdf_extract(pdf_path: str, start_on: int = 0, scheme_string: Optional[str | Callable] = None) -> list[str]:
    """ Given a PDF file, generates a new folder with all extracted images

    Code adapted from Kraken 4.3.1

    :param pdf_path:
    :param start_on: Page to start on (Default is 2 because Gallica adds generated metadata)
    :param scheme_string: String Scheme
    :return:
    """
    doc = fitz.open(pdf_path)
    n_pages = len(doc)
    if not scheme_string:
        scheme_string = pdf_name_scheme(pdf_path)
    elif callable(scheme_string):
        scheme_string = scheme_string(pdf_path)
    out = []
    for i in range(start_on, n_pages):
        page = doc.load_page(i)
        pix = page.get_pixmap(matrix=fitz.Matrix(300 / 72, 300 / 72)) # scaling dpi
        local_targ = scheme_string.format(i)
        pix.save(local_targ)
        out.append(str(local_targ))
    doc.close()
    return out


def pdf_name_scheme(pdf_path: str, output_dir: Optional[str] = None, page_prefix: str = "f") -> str:
    """ Generate a file scheme based on a PDF file

    :param pdf_path: Path to the PDF
    :param output_dir: Path where the output should be created, by default write in the same path as PDF
    :param page_prefix: The prefix for the file export, by default "f"
    :return: F-String

    >>> pdf_name_scheme("check.pdf")
    'check/f{}.jpg'
    >>> pdf_name_scheme("blop/check.pdf")
    'blop/check/f{}.jpg'
    >>> pdf_name_scheme("check.pdf", output_dir="output")
    'output/check/f{}.jpg'
    >>> pdf_name_scheme("blop/check.pdf", output_dir="output")
    'output/check/f{}.jpg'
    >>> pdf_name_scheme("check.pdf", page_prefix='p')
    'check/p{}.jpg'

    """
    path = Path(pdf_path)
    if output_dir:
        target = Path(os.path.join(output_dir, path.stem))
    else:
        target = Path(os.path.join(path.parent, path.stem))
    os.makedirs(target, exist_ok=True)
    return str(Path.joinpath(target, page_prefix + "{}.jpg"))


def pdf_get_nb_pages(pdf_path: str) -> int:
    doc = fitz.open(pdf_path)
    page_count = len(doc)
    doc.close()
    return page_count


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


def cleverer_manifest_parsing(image: Dict[str, Any], head_check: bool = False) -> Optional[str]:
    """ More robust parsing of manifests that should handle most implementation of IIIF. Takes an image dict and
        returns a URL (None if could not be resolved)

    Original source and Copyright: https://github.com/ecoto/iiif_downloader/blob/master/iiif_downloader.py

    :param image:
    :param head_check: Runs a HEAD request on the image to check for extension (Defaults to NONE and JPG)
    :return:
    """
    if 'resource' in image and (('format' in image['resource'] and 'image' in image['resource']['format']) or
                                ('@type' in image['resource'] and image['resource']['@type'] == 'dctypes:Image')):
        if 'service' in image['resource']:
            # check the context for the API version
            if '@context' in image['resource']['service'] and '/1/' in image['resource']['service']['@context']:
                # attempt to retrieve files named 'native' if API v1.1 is used
                image_url = image['resource']['service']['@id'] + '/full/full/0/native'
            else:
                # attempt to retrieve files named 'default' otherwise
                image_url = image['resource']['service']['@id'] + '/full/full/0/default'
            # avoid an (occasionally) incorrect double // when building the URL
            image_url = image_url.replace('//full', '/full')
            # check if image can be downloaded without specifying the format...
            if head_check:
                head_response = requests.head(image_url, allow_redirects=True, verify=True)
                if head_response.status_code != 200:
                    # ... try get the format otherwise
                    response = requests.get(image['resource']['service']['@id'] + '/info.json', allow_redirects=True)
                    service_document = response.json()
                    if len(service_document['profile']) > 1:
                        service_profiles = service_document['profile'][1:]  # 0 is always a compliance URL
                        if 'formats' in service_profiles[0]:
                            image_format = service_profiles[0]['formats'][0]  # just use the first format
                            return image_url + '.' + image_format
                        return image['resource']['@id']
                    return image['resource']['@id']
            return image_url+".jpg"
        return image['resource']['@id']
    return None


def clean_kebab(string: str) -> str:
    return cases.to_kebab(unidecode.unidecode(string))
