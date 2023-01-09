from typing import Tuple
import requests
import os
import lxml.etree as ET


def download(param: Tuple[str, str]) -> str:
    url, target = param
    os.makedirs(os.path.dirname(target), exist_ok=True)
    response = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/51.0.2704.103 Safari/537.36"})
    with open(target, 'wb') as handle:
        handle.write(response.content)
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