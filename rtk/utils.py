from typing import Tuple, List, Optional
import requests
import os
import hashlib
import csv
import lxml.etree as ET


def download(param: Tuple[str, str]) -> str:
    url, target = param
    os.makedirs(os.path.dirname(target), exist_ok=True)
    response = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/51.0.2704.103 Safari/537.36"})
    try:
        response.raise_for_status()
        with open(target, 'wb') as handle:
            handle.write(response.content)
        return target
    except Exception as E:
        print(E)
        return None


def download_iiif_manifest(param: Tuple[str, str]) -> str:
    url, output_file = param
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    response = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/51.0.2704.103 Safari/537.36"})
    response.raise_for_status()
    j = response.json()
    rows = []
    dirname = os.path.splitext(os.path.basename(output_file))[0]
    if "items" in j:
        for element in j["items"]:
            rows.append([element["items"][0]["items"][0]["body"]["id"], dirname])
    elif "sequences" in j:
        for element in j["sequences"][0]["canvases"]:
            rows.append([element["images"][0]["resource"]["@id"], dirname])

    with open(output_file, 'w') as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)

    return url


def check_content(filepath, ratio: float = .9):
    """ Check that filepath has at least N content done

    :param filepath:
    :param ratio:
    :return:
    """
    try:
        xml = ET.parse(filepath)
    except Exception:
        return False
    data = []
    for content in xml.xpath("//a:String/@CONTENT", namespaces={"a": "http://www.loc.gov/standards/alto/ns-v4#"}):
        data.append(int(bool(str(content))))
    return (sum(data) / (len(data) or 1)) > ratio


def clean_kraken_filename(filepath):
    """
    >>> clean_kraken_filename("../test_dir/AEV_3090_1870_Goms_Ausserbinn_001.xml")
    """
    try:
        xml = ET.parse(filepath)
        for content in xml.xpath("//a:fileName", namespaces={"a": "http://www.loc.gov/standards/alto/ns-v4#"}):
            content.text = os.path.basename(content.text)
    except Exception:
        return False

    with open(filepath, "w") as f:
        f.write(ET.tostring(xml, encoding=str))

    return filepath


def check_kraken_filename(filepath):
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
