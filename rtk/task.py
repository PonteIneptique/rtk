import os
import pathlib
import subprocess
import csv
from typing import Dict, Union, Tuple, List, Optional, Callable, Literal
from concurrent.futures import ThreadPoolExecutor
import re
from xml.sax import saxutils
# Non Std Lib
import requests
import tqdm
# Local
from rtk import utils


InputType = Union[str, Tuple[str, str]]
InputListType = Union[List[str], List[Tuple[str, str]]]
DownstreamCheck = Optional[Callable[[InputType], bool]]


def _sbmsg(msg) -> str:
    return f"\t[Subtask] {msg}"


class Task:
    def __init__(self,
                 input_files: InputListType,
                 command: Optional[str] = None,
                 multiprocess: Optional[int] = None,
                 **options
                 ):
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


class DownloadIIIFImageTask(Task):
    """ Downloads IIIF images

    Downloads an image and takes a first input string (URI) and a second one (Directory) [Optional]

    """
    def __init__(
            self,
            *args,
            downstream_check: DownstreamCheck = None,
            max_height: Optional[int] = None,
            max_width: Optional[int] = None,
            custom_headers: Optional[Dict[str, str]] = None,
            **kwargs):
        super(DownloadIIIFImageTask, self).__init__(*args, **kwargs)
        self.downstream_check = downstream_check
        self._output_files = []
        self._max_h: int = max_height
        self._max_w: int = max_width
        self._custom_headers: Dict[str, str] = custom_headers or {}
        if self._max_h and self._max_w:
            raise Exception("Only one parameter max height / max width is accepted")

    @staticmethod
    def rename_download(file: InputType) -> str:
        return os.path.join(file[1], file[0].split("/")[-5] + ".jpg")

    @staticmethod
    def check_downstream_task(extension: str = ".xml", content_check: DownstreamCheck = None) -> Callable:
        def check(inp):
            filename = os.path.splitext(DownloadIIIFImageTask.rename_download(inp))[0] + extension
            if not os.path.exists(filename):
                return False
            if content_check is not None:
                return content_check(filename)
            return True
        return check

    @property
    def output_files(self) -> List[InputType]:
        return self._output_files

    def check(self) -> bool:
        all_done: bool = True
        for file in tqdm.tqdm(self.input_files, desc=_sbmsg("Checking prior processed documents")):
            out_file = self.rename_download(file)
            if os.path.exists(out_file):
                self._checked_files[file] = True
                self._output_files.append(out_file)
            elif self.downstream_check is not None:  # Additional downstream check
                self._checked_files[file] = self.downstream_check(file)
                if not self._checked_files[file]:
                    all_done = False
            else:
                self._checked_files[file] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        done = []
        options = {**self._custom_headers}
        if self._max_h:
            options["max_height"] = self._max_h
        if self._max_w:
            options["max_width"] = self._max_w
        try:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                bar = tqdm.tqdm(total=len(inputs), desc=_sbmsg("Downloading..."))
                for file in executor.map(
                    utils.simple_args_kwargs_wrapper(utils.download_iiif_image, options=options),
                    [(file[0], self.rename_download(file)) for file in inputs]
                ):  # urls=[list of url]
                    bar.update(1)
                    if file:
                        done.append(file)
        except KeyboardInterrupt:
            bar.close()
            print("Download manually interrupted, removing partial JPGs")
            for url, directory in inputs:
                if url not in done:
                    tgt = self.rename_download((url, directory))
                    if os.path.exists(tgt):
                        os.remove(tgt)
        self._output_files.extend(done)
        return True


class ExtractPDFTask(Task):
    def __init__(
            self,
            *args,
            **kwargs):
        super(ExtractPDFTask, self).__init__(*args, **kwargs)
        self._output_files: List[str] = []

    def check(self) -> bool:
        all_done: bool = True
        for inp in tqdm.tqdm(
                self.input_files,
                desc=_sbmsg("Checking prior processed documents"),
                total=len(self.input_files)
        ):
            pdf_nb_pages = utils.pdf_get_nb_pages(inp)
            scheme = utils.pdf_name_scheme(inp)
            for page in range(pdf_nb_pages):
                out = scheme.format(page)
                if os.path.exists(out):
                    self._checked_files[inp] = True
                else:
                    self._checked_files[inp] = False
                    all_done = False
        self._output_files.extend([inp for inp, status in self._checked_files.items() if status])
        return all_done

    @property
    def output_files(self) -> List[InputType]:
        return self._output_files

    def _process(self, inputs: InputListType) -> bool:
        tp = ThreadPoolExecutor(self.workers)
        bar = tqdm.tqdm(desc=_sbmsg(f"Extract PDF images command"), total=len(inputs))
        # ToDo: Do not extract page we already have
        for fname in tp.map(utils.pdf_extract, inputs):
            self._output_files.append(fname)
            bar.update(1)
        bar.close()
        return True


class DownloadGallicaPDF(Task):
    """ Downloads Gallica PDF after a manifest has been downloaded

    This does use manifest URIs and is specific to Gallica.

    As of early january 2023, Gallica closed or heavily reduced

    :param manifest_as_directory: Boolean that uses the manifest filename (can be a function) as a directory container
    """

    SCHEME_ADDRESS = "https://gallica.bnf.fr/ark:/12148/{ark}/f1n{length}.pdf"
    GET_ARK = re.compile(r"ark:\/\w+/(\w+)")

    def __init__(
            self,
            *args,
            manifest_task: "DownloadIIIFManifestTask",
            naming_function: Optional[Callable[[str], str]] = None,
            output_directory: Optional[str] = None,
            **kwargs):
        super(DownloadGallicaPDF, self).__init__(*args, **kwargs)
        self.length_dict: Dict[str, int] = manifest_task.get_output_length_dict()
        self.naming_function = naming_function or self.ark
        self.output_directory = output_directory or "."
        self._output_files: List[InputType] = []

    @staticmethod
    def ark(manifest_uri: str) -> str:
        """ Get the ark last id

        :param manifest_uri:
        :return:

        >>> DownloadGallicaPDF.ark("https://gallica.bnf.fr/iiif/ark:/12148/btv1b90601825/manifest.json")
        'btv1b90601825'
        """
        return DownloadGallicaPDF.GET_ARK.findall(manifest_uri)[0]

    def rename_download(self, file: InputType) -> str:
        return os.path.join(
            self.output_directory,
            utils.change_ext(self.naming_function(file), "pdf")
        )

    @property
    def output_files(self) -> List[InputType]:
        """ Returns the PDF path for each input
        """
        return self._output_files

    @staticmethod
    def download_pdf(manifest_and_target_and_length: Tuple[str, str, int]) -> str:
        man, targ, length = manifest_and_target_and_length
        ark = DownloadGallicaPDF.ark(man)
        resp = requests.get(DownloadGallicaPDF.SCHEME_ADDRESS.format(
            ark=ark,
            length=length
        ))
        with open(targ, "wb") as f:
            f.write(resp.content)
        return targ

    def check(self) -> bool:
        all_done: bool = True
        for file in tqdm.tqdm(self.input_files, desc=_sbmsg("Checking prior processed documents")):
            out_file = self.rename_download(file)
            if os.path.exists(out_file):
                self._checked_files[file] = True
                self._output_files.append(out_file)
            else:
                self._checked_files[file] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            bar = tqdm.tqdm(total=len(inputs), desc=_sbmsg("Downloading..."))
            for file in executor.map(self.download_pdf, [
                (file, self.rename_download(file), self.length_dict[file])
                for file in inputs
            ]):  # urls=[list of url]
                bar.update(1)
                self._output_files.append(file)
        return True


class DownloadIIIFManifestTask(Task):
    """ Downloads IIIF manifests

    Download task takes a first input string (URI)

    :param manifest_as_directory: Boolean that uses the manifest filename (can be a function) as a directory container
    """
    def __init__(
            self,
            *args,
            naming_function: Optional[Callable[[str], str]] = None,
            output_directory: Optional[str] = None,
            custom_headers: Optional[Dict[str, str]] = None,
            **kwargs):
        super(DownloadIIIFManifestTask, self).__init__(*args, **kwargs)
        self.naming_function = naming_function or utils.string_to_hash
        self.output_directory = output_directory or "."
        self._custom_headers: Dict[str, str] = custom_headers or {}

    def rename_download(self, file: InputType) -> str:
        return os.path.join(self.output_directory, utils.change_ext(self.naming_function(file), "csv"))

    def get_output_length_dict(self) -> Dict[InputType, int]:
        """ Method to simply access manifest length and not duplicate the method

        :return:
        """
        out = {}
        for file in self.input_files:
            dl_file = self.rename_download(file)
            if os.path.exists(dl_file):
                with open(dl_file) as f:
                    out[file] = len(list([0 for _ in csv.reader(f)]))
        return out

    @property
    def output_files(self) -> List[InputType]:
        """ Unlike the others, one input file = more output files

        We read inputfile transformed to get the output files (CSV files: FILE + Directory)
        """
        out = []
        for file in self.input_files:
            dl_file = self.rename_download(file)
            if os.path.exists(dl_file):
                with open(dl_file) as f:
                    files = list([tuple(row) for row in csv.reader(f)])
                out.extend(files)
        return out

    def check(self) -> bool:
        all_done: bool = True
        for file in tqdm.tqdm(self.input_files, desc=_sbmsg("Checking prior processed documents")):
            out_file = self.rename_download(file)
            if os.path.exists(out_file):
                self._checked_files[file] = True
            else:
                self._checked_files[file] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        done = []
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            bar = tqdm.tqdm(total=len(inputs), desc=_sbmsg("Downloading..."))
            for file in executor.map(
                utils.simple_args_kwargs_wrapper(utils.download_iiif_manifest, options=self._custom_headers),
                [(file, self.rename_download(file)) for file in inputs]):  # urls=[list of url]
                bar.update(1)
                if file:  # Ensure we downloaded the file
                    done.append(file)
        return True


class KrakenLikeCommand(Task):
    """ Runs a Kraken Like command (Kraken, YALTAi)

    KrakenLikeCommand expect `$out` in its command
    """
    def __init__(
            self,
            *args,
            output_format: Optional[str] = "xml",
            desc: Optional[str] = "kraken-like",
            allow_failure: bool = True,
            check_content: bool = False,
            **kwargs):
        super(KrakenLikeCommand, self).__init__(*args, **kwargs)
        self._output_format: str = output_format
        self.check_content: bool = check_content
        self.allow_failure: bool = allow_failure
        self._output_files: List[str] = []
        self.desc: str = desc
        if "$out" not in self.command:
            raise NameError("$out is missing in the Kraken-like command")

    def rename(self, inp):
        return os.path.splitext(inp)[0] + "." + self._output_format

    @property
    def output_files(self) -> List[InputType]:
        return list([
            self.rename(file)
            for file in self._output_files
        ])

    def check(self) -> bool:
        all_done: bool = True
        for inp in tqdm.tqdm(
                self.input_files,
                desc=_sbmsg("Checking prior processed documents"),
                total=len(self.input_files)
        ):
            out = self.rename(inp)
            if os.path.exists(out):
                self._checked_files[inp] = not self.check_content or utils.check_content(out)
            else:
                self._checked_files[inp] = False
                all_done = False
        self._output_files.extend([inp for inp, status in self._checked_files.items() if status])
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        """ Use parallel """
        def work(sample):
            proc = subprocess.Popen(
                self.command
                    .replace("$out", self.rename(sample))
                    .replace("$", sample),
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            proc.wait()
            print("Error detected in subprocess...")
            print(proc.stdout.read().decode())
            print(proc.stderr.read().decode())

            if proc.returncode == 1:
                print("Error detected in subprocess...")
                print(proc.stdout.read().decode())
                print(proc.stderr.read().decode())
                print("Stopped process")
                if not self.allow_failure:
                    raise InterruptedError
                return None
            return sample

        tp = ThreadPoolExecutor(self.workers)
        bar = tqdm.tqdm(desc=_sbmsg(f"Processing {self.desc} command"), total=len(inputs))
        for fname in tp.map(work, inputs):
            if fname is not None:
                self._output_files.append(fname)
            bar.update(1)
        bar.close()


class YALTAiCommand(KrakenLikeCommand):
    """ Runs a Kraken recognizer

    KrakenLikeCommand expect `$out` in its command
    """
    def __init__(
            self,
            *args,
            yoloV5_model: Union[str, pathlib.Path],
            line_model: Optional[Union[str, pathlib.Path]] = None,
            device: str = "cpu",
            allow_failure: bool = False,
            check_content: bool = False,
            binary: str = "yaltai",  # Environment can be env/bin/yaltai
            **kwargs):
        if not os.path.exists(yoloV5_model):
            raise ValueError(f"Unknown YOLOv5 model `{yoloV5_model}`")

        cmd = f"{binary} kraken {' --verbose ' if kwargs.get('verbose') else ''} {' --raise-on-error ' if kwargs.get('raise-on-error') else ''} -i $ $out --device {device} segment -y {yoloV5_model}"

        if line_model:
            if not os.path.exists(line_model):
                raise ValueError(f"Unknown YOLOv5 model `{line_model}`")
            cmd += f" -i {line_model}"
        else:
            print("Using default Kraken line segmenter.")

        super(YALTAiCommand, self).__init__(
            *args,
            command=cmd,
            allow_failure=allow_failure,
            output_format="xml",
            check_content=check_content,
            desc="YALTAi segmenter",
            **kwargs
        )


class KrakenRecognizerCommand(KrakenLikeCommand):
    """ Runs a Kraken recognizer

    KrakenLikeCommand expect `$out` in its command
    """
    def __init__(
            self,
            *args,
            model: Union[str, pathlib.Path],
            device: str = "cpu",
            raise_on_error: bool = False,
            input_format: Optional[str] = "alto",
            check_content: bool = False,
            binary: str = "kraken",  # Environment can be env/bin/kraken
            **kwargs):
        if not os.path.exists(model):
            raise ValueError(f"Unknown Kraken model `{model}`")
        options = ""
        if raise_on_error:
            options += " --raise-on-error "
        super(KrakenRecognizerCommand, self).__init__(
            *args,
            command=f"{binary} {options} -i $ $out --device {device} -f xml --{input_format} ocr -m {model}",
            allow_failure=not raise_on_error,
            output_format="xml",
            check_content=check_content,
            desc="Kraken recognizer",
            **kwargs
        )


class KrakenAltoCleanUpCommand(Task):
    """ Clean-up Kraken Serialization

    The Kraken output serialization is not compatible with its input serialization
    """

    @property
    def output_files(self) -> List[InputType]:
        return self.input_files

    def check(self) -> bool:
        all_done: bool = True
        for inp in tqdm.tqdm(
                self.input_files, desc=_sbmsg("Checking prior processed documents"), total=len(self.input_files)):
            if os.path.exists(inp):
                # ToDo: Check XML or JSON is well-formed
                self._checked_files[inp] = utils.check_kraken_filename(inp)
            else:
                self._checked_files[inp] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        done = []
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            bar = tqdm.tqdm(total=len(inputs), desc=_sbmsg("Cleaning..."))
            for file in executor.map(utils.clean_kraken_filename, inputs):  # urls=[list of url]
                bar.update(1)
                done.append(file)
        return True


class ClearFileCommand(Task):
    """ Remove files when they have been processed, useful for JPG

    """
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


class ExtractZoneAltoCommand(Task):
    """ This command takes an ALTO input and transforms it into a .txt file, only keeping the provided Zones.
    """
    def __init__(
            self,
            *args,
            zones: Optional[List[str]],
            fmt: Literal["txt", "tei"] = "txt",
            **kwargs):
        super(ExtractZoneAltoCommand, self).__init__(*args, **kwargs)
        self.zones = zones
        self.fmt = fmt

    def rename(self, inp):
        return os.path.splitext(inp)[0] + "." + self.fmt

    @property
    def output_files(self) -> List[InputType]:
        return list([self.rename(file) for file in self.input_files])

    def check(self) -> bool:
        all_done: bool = True
        for file in tqdm.tqdm(self.input_files, desc=_sbmsg("Checking prior processed documents")):
            if os.path.exists(self.rename(file)):
                self._checked_files[file] = True
            else:
                self._checked_files[file] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        def custom_alto_zone_extraction(input_file):
            content = utils.alto_zone_extraction(input_file, self.zones)
            if content:
                with open(self.rename(input_file), "w") as f:
                    if self.fmt == "txt":
                        f.write("\n".join([
                            line
                            for zone in content
                            for line in zone["lines"]
                        ]))
                    else:
                        text = "\n"
                        for zone in content:
                            text += f"<div type='{zone['type']}'>\n"
                            for line in zone["lines"]:
                                text += f"    <seg><lb />{saxutils.escape(line)}</seg>\n"
                            text += "</div>\n"
                        f.write(text)

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            bar = tqdm.tqdm(total=len(inputs), desc=_sbmsg("Cleaning..."))
            for file in executor.map(custom_alto_zone_extraction, inputs):  # urls=[list of url]
                bar.update(1)
        return True
