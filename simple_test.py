from rtk.task import DownloadTask, KrakenLikeCommand, KrakenAltoCleanUp, ClearFileCommand
from rtk import utils


with open("test.txt") as f:
    text = f.read().split()

batch_size = 10
batches = [text[n:n+batch_size] for n in range(0, len(text), batch_size)]

for batch in batches:
    # Download Files
    print("[Task] Download JPG")
    dl = DownloadTask(
        [(f, "test_dir/") for f in batch],
        multiprocess=4,
        completion_check=DownloadTask.check_downstream_task("xml", utils.check_content)
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
