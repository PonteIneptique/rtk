""" This is a sample script for using RTK (Release the krakens)

It takes a file with a list of images to download from IIIF (See test.txt) and passes it in a suit of commands:

1. It downloads them
2. It applies YALTAi segmentation with line segmentation
3. It fixes up the image PATH of XML files
4. It processes the text as well through Kraken
5. It removes the image files (from the one hunder object that were meant to be done in group)

The batch file should be lower if you want to keep the space used low, specifically if you use DownloadIIIFManifest.

"""
from rtk.task import DownloadIIIFImageTask, KrakenLikeCommand, KrakenAltoCleanUp, ClearFileCommand
from rtk import utils

batches = utils.batchify_textfile("test.txt", batch_size=100)

for batch in batches:
    # Download Files
    print("[Task] Download JPG")
    dl = DownloadIIIFImageTask(
        [(f, "test_dir/") for f in batch],
        multiprocess=4,
        completion_check=DownloadIIIFImageTask.check_downstream_task("xml", utils.check_content)
    )
    dl.process()

    # Apply YALTAi
    print("[Task] Segment")
    yaltai = KrakenLikeCommand(
        dl.output_files,
        command="yaltaienv/bin/yaltai kraken -i $ $out --device cuda:0 "
                "segment "
                "-y /home/thibault/Downloads/YALTAiSegmentationZone.pt "
                "-i /home/thibault/Downloads/KrakenSegmentationLigne.mlmodel",
        multiprocess=4,  # GPU Memory // 5gb
        desc="YALTAi"
    )
    yaltai.process()

    # Clean-up the relative filepath of Kraken Serialization
    print("[Task] Clean-Up Serialization")
    cleanup = KrakenAltoCleanUp(yaltai.output_files)
    cleanup.process()

    # Apply Kraken
    print("[Task] OCR")
    kraken = KrakenLikeCommand(
        yaltai.output_files,
        command="krakenv/bin/kraken -i $ $out --device cuda:0 -f xml --alto "
                "ocr -m /home/thibault/Downloads/KrakenTranscription.mlmodel",
        multiprocess=4,  # GPU Memory // 3gb
        desc="Kraken",
        check_content=True
    )
    kraken.process()

    print("[Task] Remove images")
    cf = ClearFileCommand(dl.output_files, multiprocess=4).process()
