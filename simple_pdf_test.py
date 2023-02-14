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
from rtk.task import DownloadGallicaPDF, ExtractPDFTask, KrakenLikeCommand, KrakenAltoCleanUpCommand, ClearFileCommand, \
    DownloadIIIFManifestTask
from rtk import utils

batches = utils.batchify_textfile("manifests.txt", batch_size=2)

for batch in batches:
    # Download Manifests
    print("[Task] Download manifests")
    manifests = DownloadIIIFManifestTask(
        batch,
        output_directory="test_pdf",
        naming_function=lambda x: x.split("/")[-2], multiprocess=10
    )
    manifests.process()

    # Download PDF
    print("[Task] Download JPG")
    dl = DownloadGallicaPDF(
        batch,
        output_directory="test_pdf",
        manifest_task=manifests,
        multiprocess=4
    )
    dl.process()

    # Extract images !
    print("[Task] Download PDF")
    extr = ExtractPDFTask(
        dl.output_files,
        multiprocess=2
    )
    extr.process()

    # Apply YALTAi
    print("[Task] Segment")
    yaltai = KrakenLikeCommand(
        extr.output_files,
        command="yaltaienv/bin/yaltai kraken -i $ $out --device cuda:0 "
                "segment -y GallicorporaSegmentation.pt ",
        multiprocess=4,  # GPU Memory // 5gb
        desc="YALTAi"
    )
    yaltai.process()

    raise Exception
    # Clean-up the relative filepath of Kraken Serialization
    print("[Task] Clean-Up Serialization")
    cleanup = KrakenAltoCleanUpCommand(yaltai.output_files)
    cleanup.process()

    # Apply Kraken
    print("[Task] OCR")
    kraken = KrakenLikeCommand(
        yaltai.output_files,
        command="krakenv/bin/kraken -i $ $out --device cuda:0 -f xml --alto "
                "ocr -m cremma-medieval_best.mlmodel",
        multiprocess=4,  # GPU Memory // 3gb
        desc="Kraken",
        check_content=True
    )
    kraken.process()

    #print("[Task] Remove images")
    #cf = ClearFileCommand(dl.output_files, multiprocess=4).process()
