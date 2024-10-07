""" This is a sample script for using RTK (Release the krakens)

It takes a file with a list of manifests to download from IIIF (See manifests.txt) and passes it in a suit of commands:

0. It downloads manifests and transform them into CSV files
1. It downloads images from the manifests
2. It applies YALTAi segmentation with line segmentation
3. It fixes up the image PATH of XML files
4. It processes the text as well through Kraken
5. It removes the image files (from the one hunder object that were meant to be done in group)

The batch file should be lower if you want to keep the space used low, specifically if you use DownloadIIIFManifest.

"""
from rtk.task import KrakenAltoCleanUpCommand, ClearFileCommand, YALTAiCommand, KrakenRecognizerCommand, ExtractPDFTask
from rtk import utils
import os
import zipfile
import json
import torch

def read_manifest_identifiers(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Function to create zip files
def create_zip_files(identifier, output_directory, image_folder):
    image_zip_path = os.path.join(output_directory, f"{identifier}_facsimile.zip")
    with zipfile.ZipFile(image_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(image_folder):
            for file in files:
                if file.endswith('.jpg'):
                    zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), image_folder))

    # Create zip for XMLs
    xml_zip_path = os.path.join(output_directory, f"{identifier}_altos_transcribed.zip")
    with zipfile.ZipFile(xml_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(image_folder):
            for file in files:
                if file.endswith('.xml'):
                    zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), image_folder))

manifest_identifiers, batches = utils.batchify_jsonfile('pdfs.json', batch_size=2)

from re import sub


def kebab(s, max_length=100):
    kebab_string = sub(r".pdf", "", '-'.join(
        sub(r"(\W+)"," ",
        sub(r"[A-Z]{2,}(?=[A-Z][a-z]+[0-9]*|\b)|[A-Z]?[a-z]+[0-9]*|[A-Z]|[0-9]+",
        lambda mo: ' ' + mo.group(0).lower(), s)).split()
    ))
    # trunc
    if len(kebab_string) > max_length:
        kebab_string = kebab_string[:max_length].rsplit('-', 1)[0]  # Truncate the last hyphen to avoid a break in the middle of the word
    return kebab_string


for batch in batches:
    # Download Manifests
    print("[Task] Extract PDFs")
    dl = ExtractPDFTask(
        batch,
        output_directory="output",
        naming_function=lambda x: kebab(x), multiprocess=10
    )
    dl.process()

    # Apply YALTAi
    print("[Task] Segment")
    yaltai = YALTAiCommand(
        dl.output_files,
        binary="yaltai",
        device="cuda:0",
        yolo_model="seg_model.pt",
        raise_on_error=True,
        allow_failure=False,
        multiprocess=4,  # GPU Memory // 5gb
        check_content=False
    )
    yaltai.process()

    torch.cuda.empty_cache()

    # Clean-up the relative filepath of Kraken Serialization
    print("[Task] Clean-Up Serialization")
    cleanup = KrakenAltoCleanUpCommand(yaltai.output_files)
    cleanup.process()

    # Apply Kraken
    print("[Task] OCR")
    kraken = KrakenRecognizerCommand(
        yaltai.output_files,
        binary="kraken",
        device="cuda:0",
        model="nfd_best.mlmodel",
        multiprocess=8,  # GPU Memory // 3gb
        check_content=False
    )
    kraken.process()

    torch.cuda.empty_cache()

    for pdf in batch:
        identifier = manifest_identifiers.get(pdf)
        if identifier:
            image_folder = os.path.join(kebab(pdf))
            if image_folder:
                create_zip_files(identifier, "output", image_folder)
