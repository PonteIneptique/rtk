from typing import Dict, Union, Tuple, List, Optional
import os
from concurrent.futures import ThreadPoolExecutor
import tqdm
from rtk.utils import download


InputType = Union[str, Tuple[str, str]]
InputListType = Union[List[str], List[Tuple[str, str]]]


class Task:
    def __init__(self,
                 input_files: InputListType,
                 command: Optional[str] = None,
                 multiprocess: Optional[int] = None
                 ) -> "Task":
        self._input_files: InputListType = input_files
        self._command: Optional[str] = command
        self._checked_files: Dict[InputType, bool] = {}
        self._workers: int = multiprocess or 1

    def check(self) -> bool:
        raise NotImplementedError

    def process(self) -> bool:
        self.check()
        requires_processing = [
            file for file, status in self._checked_files.items()
            if not status
        ]
        return self._process(requires_processing)

    def _process(self, inputs: InputListType) -> bool:
        raise NotImplementedError

    @property
    def output_files(self) -> List[str]:
        raise NotImplementedError


class DownloadTask(Task):
    """ Download task takes a first input string (URI) and a second one (Directory)

    """
    @staticmethod
    def _rename_download(file: InputType) -> str:
        return os.path.join(file[1], file[0].split("/")[-5] + ".jpg")

    @property
    def output_files(self) -> List[InputType]:
        return list([
            self._rename_download(file)
            for file in self._input_files
        ])

    def check(self) -> bool:
        all_done: bool = True
        for file in tqdm.tqdm(self._input_files, desc="Checking prior processed documents"):
            out_file = self._rename_download(file)
            if os.path.exists(out_file):
                self._checked_files[file] = True
            else:
                self._checked_files[file] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        done = []
        try:
            with ThreadPoolExecutor(max_workers=self._workers) as executor:
                bar = tqdm.tqdm(total=len(inputs), desc="Downloading...")
                for file in executor.map(download, [
                    (file[0], self._rename_download(file))
                    for file in inputs
                ]):  # urls=[list of url]
                    bar.update(1)
                    done.append(file)
        except KeyboardInterrupt:
            bar.close()
            print("Download manually interrupted, removing partial JPGs")
            for url, directory in inputs:
                if url not in done:
                    tgt = self._rename_download((url, directory))
                    if os.path.exists(tgt):
                        os.remove(tgt)
        return True


class KrakenLikeCommand(Task):
    """ Apply kraken or yaltai command

    """
    @staticmethod
    def _rename_download(file: InputType) -> str:
        return os.path.join(file[1], file[0].split("/")[-5] + ".jpg")

    @property
    def output_files(self) -> List[InputType]:
        return list([
            self._rename_download(file)
            for file in self._input_files
        ])

    def check(self) -> bool:
        all_done: bool = True
        for file in tqdm.tqdm(self._input_files, desc="Checking prior processed documents"):
            out_file = self._rename_download(file)
            if os.path.exists(out_file):
                self._checked_files[file] = True
            else:
                self._checked_files[file] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        done = []
        try:
            with ThreadPoolExecutor(max_workers=self._workers) as executor:
                bar = tqdm.tqdm(total=len(inputs), desc="Downloading...")
                for file in executor.map(download, [
                    (file[0], self._rename_download(file))
                    for file in inputs
                ]):  # urls=[list of url]
                    bar.update(1)
                    done.append(file)
        except KeyboardInterrupt:
            bar.close()
            print("Download manually interrupted, removing partial JPGs")
            for url, directory in inputs:
                if url not in done:
                    tgt = self._rename_download((url, directory))
                    if os.path.exists(tgt):
                        os.remove(tgt)
        return True