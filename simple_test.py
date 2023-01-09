from rtk.task import DownloadTask, KrakenLikeCommand, CleanUpCommand


with open("test.txt") as f:
    text = f.read().split()

batch_size = 10
batches = [text[n:n+batch_size] for n in range(0, len(text), batch_size)]

for batch in batches:
    # Download Files
    dl = DownloadTask([(f, "test_dir/") for f in batch], multiprocess=4)
    dl.process()

    # Apply YALTAi
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
    cleanup = CleanUpCommand(yaltai.output_files)
    cleanup.process()

    # Apply Kraken
    kraken = KrakenLikeCommand(
        yaltai.output_files,
        command="krakenv/bin/kraken -i $ $out --device cuda:0 -f xml --alto "
                "ocr -m /home/thibault/Downloads/KrakenTranscription.mlmodel",
        multiprocess=4,  # GPU Memory // 3gb
        desc="Kraken",
        check_content=True
    )
    kraken.process()
