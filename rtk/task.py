from typing import Dict, Union, Tuple, List, Optional, Callable
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
import tqdm
from rtk.utils import download, check_content, clean_kraken_filename, check_kraken_filename


InputType = Union[str, Tuple[str, str]]
InputListType = Union[List[str], List[Tuple[str, str]]]
DownstreamCheck = Optional[Callable[[InputType], bool]]


def _sbmsg(msg) -> None:
    print(f"\t[Subtask] {msg}")


class Task:
    def __init__(self,
                 input_files: InputListType,
                 command: Optional[str] = None,
                 multiprocess: Optional[int] = None,
                 **options
                 ) -> "Task":
        """

        :param input_files: Name of the input files
        :param command: Replace input file by `$`, eg. `wget $ > $.txt`
        :param multiprocess: Number of process to use (default = 1)
        :param options: Task specific options
        """
        self.input_files: InputListType = input_files
        self.command: Optional[str] = command
        self._checked_files: Dict[InputType, bool] = {}
        self.workers: int = multiprocess or 1

    def check(self) -> bool:
        raise NotImplementedError

    def process(self) -> bool:
        self.check()
        requires_processing = [
            file for file, status in self._checked_files.items()
            if not status
        ]
        if not len(requires_processing):
            print("Nothing to process here.")
            return True
        return self._process(requires_processing)

    def _process(self, inputs: InputListType) -> bool:
        raise NotImplementedError

    @property
    def output_files(self) -> List[str]:
        raise NotImplementedError


class DownloadTask(Task):
    """ Download task takes a first input string (URI) and a second one (Directory)

    """
    def __init__(
            self,
            *args,
            downstream_check: DownstreamCheck = None,
            **kwargs):
        super(DownloadTask, self).__init__(*args, **kwargs)
        self.downstream_check = downstream_check

    @staticmethod
    def rename_download(file: InputType) -> str:
        return os.path.join(file[1], file[0].split("/")[-5] + ".jpg")

    @staticmethod
    def check_downstream_task(extension: str = ".xml", content_check: DownstreamCheck = None) -> Callable:
        def check(inp):
            filename = os.path.splitext(DownloadTask.rename_download(inp))[0] + extension
            if not os.path.exists(filename):
                return False
            if content_check is not None:
                return content_check(filename)
            return True
        return check

    @property
    def output_files(self) -> List[InputType]:
        return list([
            self.rename_download(file)
            for file in self.input_files
        ])

    def check(self) -> bool:
        all_done: bool = True
        for file in tqdm.tqdm(self.input_files, desc=_sbmsg("Checking prior processed documents")):
            out_file = self.rename_download(file)
            if os.path.exists(out_file):
                self._checked_files[file] = True
            elif self.downstream_check is not None:  # Additional downstream check
                self._checked_files[file] = self.downstream_check(out_file)
                if not self._checked_files[file]:
                    all_done = False
            else:
                self._checked_files[file] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        done = []
        try:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                bar = tqdm.tqdm(total=len(inputs), desc=_sbmsg("Downloading..."))
                for file in executor.map(download, [
                    (file[0], self.rename_download(file))
                    for file in inputs
                ]):  # urls=[list of url]
                    bar.update(1)
                    done.append(file)
        except KeyboardInterrupt:
            bar.close()
            print("Download manually interrupted, removing partial JPGs")
            for url, directory in inputs:
                if url not in done:
                    tgt = self.rename_download((url, directory))
                    if os.path.exists(tgt):
                        os.remove(tgt)
        return True


class KrakenLikeCommand(Task):
    """ Apply kraken or yaltai command

    KrakenLikeCommand expect `$out` in its command
    """
    def __init__(
            self,
            *args,
            output_format: Optional[str] = "xml",
            desc: Optional[str] = "kraken-like",
            check_content: bool = False,
            **kwargs):
        super(KrakenLikeCommand, self).__init__(*args, **kwargs)
        self._output_format: str = output_format
        self.check_content: bool = check_content
        self.desc: str = desc
        if "$out" not in self.command:
            raise NameError("$out is missing in the Kraken-like command")

    def rename(self, inp):
        return os.path.splitext(inp)[0] + "." + self._output_format

    @property
    def output_files(self) -> List[InputType]:
        return list([
            self.rename(file)
            for file in self.input_files
        ])

    def check(self) -> bool:
        all_done: bool = True
        for inp, out in tqdm.tqdm(
                zip(self.input_files, self.output_files),
                desc=_sbmsg("Checking prior processed documents"),
                total=len(self.input_files)
        ):
            if os.path.exists(out):
                # ToDo: Check XML or JSON is well-formed
                self._checked_files[inp] = check_content(out) if self.check_content else True
            else:
                self._checked_files[inp] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        """ Use parallel """
        def work(sample):
            proc = subprocess.Popen(
                self.command
                    .replace("$out", self.rename(sample))
                    .replace("$", sample),
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT
            )
            proc.wait()
            if proc.returncode == 1:
                print("Error detected in subprocess...")
                print(proc.stderr.read())
                print("Stopped process")
                raise InterruptedError
            return sample

        tp = ThreadPoolExecutor(self.workers)
        bar = tqdm.tqdm(desc=_sbmsg(f"Processing {self.desc} command"), total=len(inputs))
        for _ in tp.map(work, inputs):
            bar.update(1)
        bar.close()


class KrakenAltoCleanUp(Task):
    """ Executes a single function on a specific file
    """
    @property
    def output_files(self) -> List[InputType]:
        return self.input_files

    def check(self) -> bool:
        all_done: bool = True
        for inp in tqdm.tqdm(self.input_files, desc=_sbmsg("Checking prior processed documents")
                , total=len(self.input_files)):
            if os.path.exists(inp):
                # ToDo: Check XML or JSON is well-formed
                self._checked_files[inp] = check_kraken_filename(inp)
            else:
                self._checked_files[inp] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        done = []
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            bar = tqdm.tqdm(total=len(inputs), desc=_sbmsg("Cleaning..."))
            for file in executor.map(clean_kraken_filename, inputs):  # urls=[list of url]
                bar.update(1)
                done.append(file)
        return True


class ClearFileCommand(Task):
    @property
    def output_files(self) -> List[InputType]:
        return []

    def check(self) -> bool:
        all_done: bool = True
        for file in tqdm.tqdm(self.input_files, desc=_sbmsg("Checking prior processed documents")):
            if not os.path.exists(file):
                self._checked_files[file] = True
            else:
                self._checked_files[file] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            bar = tqdm.tqdm(total=len(inputs), desc=_sbmsg("Cleaning..."))
            for file in executor.map(os.remove, inputs):  # urls=[list of url]
                bar.update(1)
        return True
