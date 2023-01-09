from rtk.task import DownloadTask

text = open("test.txt").read().split()
batch1 = text[:100]
batch2 = text[100:]
dl = DownloadTask([(f, "test_dir/") for f in batch1], multiprocess=4).process()
