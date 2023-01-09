from rtk.task import DownloadTask, KrakenLikeCommand

with open("test.txt") as f:
    text = f.read().split()

batch_size = 10
batches = [text[n:n+batch_size] for n in range(0, len(text), batch_size)]

for batch in batches:
    dl = DownloadTask([(f, "test_dir/") for f in batch], multiprocess=4)
    dl.process()
    yaltai = KrakenLikeCommand(
        dl.output_files,
        command="yaltaienv/bin/yaltai kraken -i % %out --device "
                "cuda:0 segment "
                "-y /home/thibault/Downloads/YALTAiSegmentationZone.pt "
                "-i /home/thibault/Downloads/KrakenSegmentation.mlmodel",
        multiprocess=4,  # GPU Memory // 5gb
        desc="YALTAi"
    )
    yaltai.process()
    kraken = KrakenLikeCommand(
        dl.output_files,
        command="krakenv/bin/kraken -i % %out --device "
                "cuda:0 ocr "
                "-i /home/thibault/Downloads/KrakenTranscription.mlmodel",
        multiprocess=7,  # GPU Memory // 3gb
        desc="Kraken"
    )
    yaltai.process()
