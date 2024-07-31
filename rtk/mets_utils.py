# Std Lib
import os
import re
import glob
import zipfile
# Std Lib partial
from typing import Optional
from pathlib import Path
# Requirements
import lxml.etree as ET


def produce_mets(directory: str, exclude: Optional[str] = None) -> None:
    """ Generate a METS.xml file based on a directory of XML files and JPGs
    """
    mets_found = 0
    images = []
    transc = []

    def produce_image(path: str, num: int) -> str:
        """ Generate the image line """
        return f'        <file ID="image{num}">\n          <FLocat xlink:href="{Path(path).name}"/>\n        </file>'

    def produce_xml(path: str, num: int) -> str:
        """ Generate the xml line """
        return f'        <file ID="export{num}">\n          <FLocat xlink:href="{Path(path).name}"/>\n        </file>'

    def sorting_value(key):
        if "METS" in key:
            return -1
        number = re.findall(r"-f(\d+)\.xml", key)
        if number:
            return int(number[0])
        else:
            return key

    for idx, fp_t in enumerate(sorted(glob.glob(f"{directory}/*.xml"))):
        if "METS.xml" in fp_t:
            mets_found -= 1
            continue
        if exclude and exclude in fp_t:
            mets_found -= 1
            continue
        xml = ET.parse(fp_t)
        fp_i = xml.findall("//{*}fileName")[0].text
        images.append(produce_image(fp_i, idx + mets_found))
        transc.append(produce_xml(fp_t, idx + mets_found))

    groups = [
        f'      <div TYPE="page">\n        <fptr FILEID="image{idx}"/><fptr FILEID="export{idx}"/>\n      </div>'
        for idx in range(len(images))
    ]
    nl = "\n"
    METS = f"""<mets xmlns="http://www.loc.gov/METS/" xmlns:xlink="http://www.w3.org/1999/xlink">
  <fileSec>
    <fileGrp USE="image">
{nl.join(images)}
    </fileGrp>
    <fileGrp USE="base">
{nl.join(transc)}
    </fileGrp>
  </fileSec>
  <structMap TYPE="physical">
    <div TYPE="document">
{nl.join(groups)}
    </div>
  </structMap>
</mets>"""

    with open(f"{directory}/METS.xml", "w") as f:
        f.write(METS)


def zip_folder(folder_path, output_path, exclude: Optional[str] = None):
    # Get the list of files in the folder
    files = os.listdir(folder_path)
    # Sort files to add image files first and then XML files
    files.sort(key=lambda x: (x.endswith('.xml'), x.endswith('.jpg')))

    # Create a ZipFile object
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add each file to the zip file
        for file in files:
            if exclude and exclude in file:
                continue
            file_path = os.path.join(folder_path, file)
            zipf.write(file_path, arcname=file)
    return output_path

