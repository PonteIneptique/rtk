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
from rtk.task import DownloadIIIFImageTask, KrakenAltoCleanUpCommand, ClearFileCommand, \
    DownloadIIIFManifestTask, YALTAiCommand, KrakenRecognizerCommand, ExtractZoneAltoCommand
from rtk import utils

batches = utils.batchify_textfile("manifests.txt", batch_size=2)
from re import sub


def kebab(s):
    return sub(r"https?-", "", '-'.join(
        sub(r"(\W+)"," ",
        sub(r"[A-Z]{2,}(?=[A-Z][a-z]+[0-9]*|\b)|[A-Z]?[a-z]+[0-9]*|[A-Z]|[0-9]+",
        lambda mo: ' ' + mo.group(0).lower(), s)).split()
    ))


for batch in batches:
    # Download Manifests
    print("[Task] Download manifests")
    dl = DownloadIIIFManifestTask(
        batch,
        output_directory="output",
        naming_function=lambda x: kebab(x), multiprocess=10
    )
    dl.process()

    # Download Files
    print("[Task] Download JPG")
    dl = DownloadIIIFImageTask(
        dl.output_files,
        max_height=2500,
        multiprocess=4,
        downstream_check=DownloadIIIFImageTask.check_downstream_task("xml", utils.check_content)
    )
    dl.process()

    # Apply YALTAi
    print("[Task] Segment")
    yaltai = YALTAiCommand(
        dl.output_files,
        binary="yaltaienv/bin/yaltai",
        device="cuda:0",
        yolo_model="GallicorporaSegmentation.pt",
        raise_on_error=True,
        allow_failure=False,
        multiprocess=4,  # GPU Memory // 5gb
        check_content=False
    )
    yaltai.process()

    # Clean-up the relative filepath of Kraken Serialization
    print("[Task] Clean-Up Serialization")
    cleanup = KrakenAltoCleanUpCommand(yaltai.output_files)
    cleanup.process()

    # Apply Kraken
    print("[Task] OCR")
    kraken = KrakenRecognizerCommand(
        yaltai.output_files,
        binary="yaltaienv/bin/kraken",
        device="cpu",
        model="catmus-medieval.mlmodel",
        multiprocess=8,  # GPU Memory // 3gb
        check_content=False
    )
    kraken.process()

    print("[Task] Remove images")
    # cf = ClearFileCommand(dl.output_files, multiprocess=4).process()

    print("[Task] Extract")
    task = ExtractZoneAltoCommand(
        kraken.output_files,
        zones=None,
        fmt="tei"
    )
    task.process()
