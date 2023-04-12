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
from rtk.task import DownloadIIIFImageTask, YALTAiCommand, KrakenRecognizerCommand, KrakenAltoCleanUpCommand,\
    ClearFileCommand, ExtractZoneAltoCommand
from rtk import utils

batches = utils.batchify_textfile("simple_mss_test.txt", batch_size=2)

for batch in batches:
    # Download Files
    print("[Task] Download JPG")
    dl = DownloadIIIFImageTask(
        [(b, "test_mss_dir") for b in batch],
        multiprocess=4,
        max_height=2500,
        downstream_check=DownloadIIIFImageTask.check_downstream_task("xml", utils.check_content)
    )
    dl.process()

    # Apply YALTAi
    print("[Task] Segment")
    yaltai = YALTAiCommand(
        dl.output_files,
        device="cuda:0",
        yoloV5_model="GallicorporaSegmentation.pt",
        binary="yaltaienv/bin/yaltai",
        allow_failure=False,
        multiprocess=4,  # GPU Memory // 5gb
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
        model="cremma-medieval_best.mlmodel",
        multiprocess=4,  # GPU Memory // 3gb
        binary="krakenv/bin/kraken",
        check_content=True
    )
    kraken.process()

    #print("[Task] Remove images")
    #cf = ClearFileCommand(dl.output_files, multiprocess=4).process()

    print("[Task] Get text file")
    plaintxt = ExtractZoneAltoCommand(kraken.output_files, zones=["MainZone"])
    plaintxt.process()

