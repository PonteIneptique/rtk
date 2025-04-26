import os
import pathlib
import subprocess
import signal
import csv
import re
from PIL import Image
from functools import partial
from typing import Dict, Union, Tuple, List, Optional, Callable, Literal
from concurrent.futures import ThreadPoolExecutor
from xml.sax import saxutils
from collections import defaultdict
from itertools import repeat
# Non Std Lib
import requests
import tqdm
import lxml.etree as ET
# Local
from rtk import utils
from rtk import mets_utils

InputType = Union[str, Tuple[str, str]]
InputListType = Union[List[str], List[Tuple[str, str]]]
DownstreamCheck = Optional[Callable[[InputType], bool]]


def _sbmsg(msg) -> str:
    return f"[Subtask] {msg}"


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

    :param input_list: List of tuples, where the first value is a URI to download an image, and the second is a folder
    """

    def __init__(
            self,
            input_files: List[Tuple[str, str, str]],
            *args,
            output_prefix: Optional[str] = None,
            downstream_check: DownstreamCheck = None,
            max_height: Optional[int] = None,
            max_width: Optional[int] = None,
            retries: int = 1,
            retries_no_options: int = 1,
            time_between_retries: int = 30,
            custom_headers: Optional[Dict[str, str]] = None,
            **kwargs):
        super(DownloadIIIFImageTask, self).__init__(input_files=input_files, *args, **kwargs)
        self.downstream_check = downstream_check
        self.output_prefix: str = output_prefix
        self._output_files = []
        self._max_h: int = max_height
        self._max_w: int = max_width
        self.retries: int = retries
        self.retries_no_options: int = retries_no_options
        self.time_between_retries: int = time_between_retries
        self._custom_headers: Dict[str, str] = custom_headers or {}
        if self._max_h and self._max_w:
            raise Exception("Only one parameter max height / max width is accepted")
        if self.output_prefix:
            self.input_files = [
                (uri, os.path.join(output_prefix, target), fname)
                for (uri, target, fname) in self.input_files
            ]

    @staticmethod
    def rename_download(file: Tuple[str, str, str]) -> str:
        return os.path.join(file[1], f"{file[2]}.jpg")

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
                try:
                    _ = Image.open(out_file).size
                    self._checked_files[file] = True
                except Exception as E:
                    print(f"Invalid file {out_file}")
                    print(E)
                    self._checked_files[file] = False
                self._output_files.append(out_file)
            elif self.downstream_check is not None:  # Additional downstream check
                self._checked_files[file] = self.downstream_check(file)
                if not self._checked_files[file]:
                    all_done = False
            else:
                self._checked_files[file] = False
                all_done = False
        return all_done

    def _process(self, inputs: List[Tuple[str, str, str]]) -> bool:
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
                        utils.simple_args_kwargs_wrapper(utils.download_iiif_image, options=options,
                                                         retries=self.retries,
                                                         retries_no_options=self.retries_no_options,
                                                         time_between_retries=self.time_between_retries),
                        [(file[0], self.rename_download(file)) for file in inputs]
                ):  # urls=[list of url]
                    bar.update(1)
                    if file:
                        done.append(file)
        except KeyboardInterrupt:
            bar.close()
            print("Download manually interrupted, removing partial JPGs")
            for url, directory, fname in inputs:
                if url not in done:
                    tgt = self.rename_download((url, directory, fname))
                    if os.path.exists(tgt):
                        os.remove(tgt)
        self._output_files.extend(done)
        return True


class ExtractPDFTask(Task):
    """ Extract JPG from PDFs

    :param output_dir: Path to the directory containing the output of the PDF extraction
    :param start_on: Page to start the extraction from. Some PDF have added prefaces, use this for ignoring them.
    """

    def __init__(
            self,
            *args,
            output_dir: Optional[str] = None,
            start_on: int = 0,
            **kwargs):
        super(ExtractPDFTask, self).__init__(*args, **kwargs)
        self._output_files: List[str] = []
        self._output_dir: str = output_dir
        self._start_on: int = start_on

    def _get_scheme(self, pdf_path):
        return utils.pdf_name_scheme(pdf_path, output_dir=self._output_dir)

    def check(self) -> bool:
        all_done: bool = True
        pdfs_images: Dict[str, List[str]] = {}
        for single_pdf_path in tqdm.tqdm(
                self.input_files,
                desc=_sbmsg("Checking prior processed documents"),
                total=len(self.input_files)
        ):
            pdf_nb_pages = utils.pdf_get_nb_pages(single_pdf_path)
            scheme = self._get_scheme(single_pdf_path)
            pdfs_images[single_pdf_path] = []
            for page in range(self._start_on, pdf_nb_pages):
                single_page_path = scheme.format(page)
                if os.path.exists(single_page_path):
                    self._checked_files[single_pdf_path] = True
                    pdfs_images[single_pdf_path].append(single_page_path)
                else:
                    self._checked_files[single_pdf_path] = False
                    all_done = False
                    break
            if self._checked_files[single_pdf_path]:
                self._output_files.extend(pdfs_images[single_pdf_path])
        return all_done

    @property
    def output_files(self) -> List[InputType]:
        return self._output_files

    def _process(self, inputs: InputListType) -> bool:
        tp = ThreadPoolExecutor(self.workers)
        bar = tqdm.tqdm(desc=_sbmsg(f"Extract PDF images command"), total=len(inputs))

        for fname in tp.map(
                partial(utils.pdf_extract, scheme_string=self._get_scheme, start_on=self._start_on),
                inputs):
            self._output_files.extend(fname)
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
    """ Downloads IIIF manifests (list of URIs as input) and outputs (obj.output_files) a list of
     tuples such as [(uri_image_1, folder_manuscript1), (uri_image_2, folder_manuscript1),
     (uri_image_last, folder_manuscript_last)]

    :param input_files: List of manifests
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
    def output_files_map(self) -> Dict[str, str]:
        outputs = {}
        for uri in self.input_files:
            for row in (self.parse_cache(uri) or []):
                outputs[row] = uri
        return outputs

    def parse_cache(self, uri) -> Optional[List[Tuple[str, str, str]]]:
        dl_file = self.rename_download(uri)
        if os.path.exists(dl_file):
            with open(dl_file) as f:
                files = list([tuple(row) for row in csv.reader(f)])
            return files
        return None

    @property
    def output_files(self) -> List[Tuple[str, str, str]]:
        """ For each input manifest, outputs all pages found. Each page is provided with
        a directory name based on the manuscript

        We read inputfile transformed to get the output files (CSV files: FILE + Directory)
        """
        out = []
        for file in self.input_files:
            if pages := self.parse_cache(file):
                out.extend(pages)
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
            custom_check_function: Optional[Callable[[str], bool]] = None,
            max_time_per_op: int = 60,  # Seconds
            **kwargs):
        super(KrakenLikeCommand, self).__init__(*args, **kwargs)
        self.command: List[str] = [x for x in self.command if x]
        self._output_format: str = output_format
        self.check_content: bool = check_content
        self.allow_failure: bool = allow_failure
        self.check_function: Callable[[str], bool] = custom_check_function if custom_check_function is not None else utils.check_content
        self._output_files: List[str] = []
        self.max_time_per_op: int = max_time_per_op
        self.desc: str = desc
        if "R" not in self.command:
            raise NameError("R is missing in the Kraken-like command (Required for xargs)")

    def rename(self, inp):
        return os.path.splitext(inp)[0] + "." + self._output_format

    @property
    def output_files(self) -> List[InputType]:
        return list([
            self.rename(file)
            for file in self._output_files
        ])

    @staticmethod
    def pbar_parsing(input_string: str) -> List[str]:
        raise NotImplementedError

    def check(self) -> bool:
        all_done: bool = True
        for inp in tqdm.tqdm(
                self.input_files,
                desc=_sbmsg("Checking prior processed documents"),
                total=len(self.input_files)
        ):
            out = self.rename(inp)
            if os.path.exists(out):
                self._checked_files[inp] = not self.check_content or self.check_function(out)
            else:
                self._checked_files[inp] = False
                all_done = False
        self._output_files.extend([self.rename(inp) for inp, status in self._checked_files.items() if status])
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        """ Use parallel """

        def work(input_list: List[str], pbar) -> List[str]:
            cmd = []
            for x in self.command:
                if x != "R":
                    cmd.append(x)
                else:
                    cmd.extend([
                        element
                        for mapped_list in map(self.input_format, input_list)
                        for element in mapped_list
                    ])

            # This allows to control the number of threads used in a subprocess
            my_env = os.environ.copy()
            my_env["OMP_NUM_THREADS"] = "1"
            # The following values are necessary for parsing output
            my_env["LINES"] = "40"
            my_env["COLUMNS"] = "300"

            out = []

            proc = subprocess.Popen(
                cmd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=my_env,
                preexec_fn=lambda: signal.alarm(len(input_list) * self.max_time_per_op),
            )

            logs = []
            try:
                for line in iter(proc.stdout.readline, ""):
                    logs.append(line.strip())
                    for element in self.pbar_parsing(line):
                        out.append(element)
                        pbar.update(1)
                        break
                    if len(set(out)) == len(set(input_list)):
                        break

                return_code = proc.wait()

                if return_code == 1 or proc.returncode == 1:
                    print("Error detected in subprocess...")
                    print(proc.stdout.read())
                    print(proc.stderr.read())
                    print("Stopped process")
                    if not self.allow_failure:
                        raise InterruptedError
            except subprocess.TimeoutExpired as te:
                try:
                    print("\n".join(logs))
                    print(proc.stderr.read())
                    print(proc.stdout.read())
                    proc.kill()
                except Exception as E:
                    return out
                return out
            except InterruptedError:
                print("\n".join(logs))
            if out == []:
                print("\n".join(logs))
            return out

        # Group inputs into the number of workers
        inputs = list(set(inputs))
        total_texts = len(inputs)
        inputs = utils.split_batches(inputs, self.workers)

        inputs = [batches for batches in inputs if len(batches)]
        tp = ThreadPoolExecutor(len(inputs))
        bar = tqdm.tqdm(desc=_sbmsg(f"Processing {self.desc} command"), total=total_texts)

        for gen in tp.map(work, inputs, repeat(bar)):
            for elem in gen:
                if isinstance(elem, str):
                    self._output_files.append(elem)
        bar.close()

    def input_format(self, inp: str) -> List[str]:
        return ["-i", inp, self.rename(inp)]


class YALTAiCommand(KrakenLikeCommand):
    """ Runs a Kraken recognizer

    KrakenLikeCommand expect `$out` in its command

    :param input_list: List of images to process
    :type input_list: List[str]
    :param yolo_model: Path to a YOLOv8 model
    :param line_model: [Optional] Path to a custom kraken line segmentation model
    :param device: Device to run inference on
    :param allow_failure: Continues to run despite errors
    :param binary: Path to the YALTAi binary. If the same environment as RTK can be used, simply `yaltai`
    :param raise_on_error: Raise an exception on error
    :type raise_on_error: bool
    """

    def __init__(
            self,
            *args,
            yolo_model: Union[str, pathlib.Path],
            line_model: Optional[Union[str, pathlib.Path]] = None,
            device: str = "cpu",
            allow_failure: bool = False,
            check_content: bool = False,
            binary: str = "yaltai",  # Environment can be env/bin/yaltai
            **kwargs):
        if not os.path.exists(yolo_model):
            raise ValueError(f"Unknown YOLOv8 model `{yolo_model}`")

        cmd = (f"{binary} kraken --verbose --alto "
               f"{' --raise-on-error ' if kwargs.get('raise_on_error') else ''} --device {device} R "
               f"segment -y {yolo_model}").split(" ")

        if line_model:
            if not os.path.exists(line_model):
                raise ValueError(f"Unknown Kraken model `{line_model}`")
            cmd.extend(f"-i {line_model}".split(" "))
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

    @staticmethod
    def pbar_parsing(stdout: str) -> List[str]:
        return re.findall(r"Serializing as alto into (.+\.xml)\s+", stdout)


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
            template: Optional[str] = None,
            check_content: bool = False,
            binary: str = "kraken",  # Environment can be env/bin/kraken
            other_options: str = " ",
            **kwargs):
        if not os.path.exists(model):
            raise ValueError(f"Unknown Kraken model `{model}`")
        options = ""
        if raise_on_error:
            options += " --raise-on-error "

        if template:
            options += f" --template {template} "
        else:
            options += " --alto "

        options += " --format-type xml " + other_options
        super(KrakenRecognizerCommand, self).__init__(
            *args,
            command=f"{binary} {options} --device {device} --{input_format} R ocr -m {model}".split(" "),
            allow_failure=kwargs.get("allow_failure", not raise_on_error),
            output_format="xml",
            check_content=check_content,
            desc="Kraken recognizer",
            **{k: v for k, v in kwargs.items() if k != "allow_failure"}
        )

    @staticmethod
    def pbar_parsing(stdout: str) -> List[str]:
        return re.findall(r"Writing recognition results for ([^\t]+\.xml)", stdout)


class KrakenSegAndRecCommand(KrakenLikeCommand):
    """ Runs a Kraken recognizer

    KrakenLikeCommand expect `$out` in its command
    """

    def __init__(
            self,
            *args,
            htr_model: Union[str, pathlib.Path],
            seg_model: Optional[Union[str, pathlib.Path]] = None,
            device: str = "cpu",
            raise_on_error: bool = False,
            input_format: Optional[str] = "alto",
            check_content: bool = False,
            binary: str = "kraken",  # Environment can be env/bin/kraken
            **kwargs):
        if not os.path.exists(htr_model):
            raise ValueError(f"Unknown Kraken model `{htr_model}`")
        if not os.path.exists(seg_model):
            raise ValueError(f"Unknown Kraken model `{seg_model}`")

        options = ""
        if raise_on_error:
            options += " --raise-on-error "
        seg_model = f"-i {seg_model}" if seg_model else "-bl"
        super(KrakenSegAndRecCommand, self).__init__(
            *args,
            command=f"{binary} {options} --device {device} -f image --{input_format} R segment {seg_model} ocr -m {htr_model}".split(
                " "),
            allow_failure=not raise_on_error,
            output_format="xml",
            check_content=check_content,
            desc="Kraken recognizer",
            **kwargs
        )

    @staticmethod
    def pbar_parsing(stdout: str) -> List[str]:
        return re.findall(r"Writing recognition results for ([^\t]+\.xml).*✓", stdout)


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


class CleanUpAltoGlyphs(Task):
    """ This command takes an ALTO input and removes the glyph informations
    """

    def __init__(
            self,
            *args,
            keep_string: bool = True,
            **kwargs):
        super(CleanUpAltoGlyphs, self).__init__(*args, **kwargs)
        self.keep_string: bool = keep_string
        self.xsl_path: str = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            ("clean-up-alto.xsl" if keep_string else "clean-up-alto-without-string.xsl")
        )
        self._output_files = []

    def rename(self, inp):
        return inp

    @property
    def output_files(self) -> List[InputType]:
        return self._output_files

    def check(self) -> bool:
        all_done: bool = True
        for file in tqdm.tqdm(self.input_files, desc=_sbmsg("Checking prior processed documents")):
            if os.path.exists(self.rename(file)):
                with open(self.rename(file)) as f:
                    t = f.read()
                if self.keep_string and "<SP" in t:
                    self._checked_files[file] = False
                    all_done = False
                elif not self.keep_string and "<Glyph" in t:
                    self._checked_files[file] = False
                    all_done = False
                else:
                    self._checked_files[file] = True

            else:
                self._checked_files[file] = False
                all_done = False
        return all_done

    def _process(self, inputs: InputListType) -> bool:
        # Need to add batch capacities
        def apply_xslt(batch_of_files: List[str], pbar: tqdm.tqdm) -> List[str]:
            transform = ET.XSLT(ET.parse(self.xsl_path))
            out = []
            for file in batch_of_files:
                try:
                    doc = ET.parse(file)
                    doc = transform(doc)
                    doc.write(self.rename(file))
                    out.append(self.rename(file))
                except Exception as E:
                    raise E
                    print(E)
            return out

        # Group inputs into the number of workers
        total_texts = len(inputs)
        inputs = utils.split_batches(inputs, self.workers)

        tp = ThreadPoolExecutor(len([batches for batches in inputs if len(batches)]))
        bar = tqdm.tqdm(
            desc=_sbmsg(f"Removing <Glyph> from ALTO" if self.keep_string else "Removing <String> from ALTO"),
            total=total_texts
        )

        for gen in tp.map(apply_xslt, inputs, repeat(bar)):
            for elem in gen:
                if isinstance(elem, str):
                    self._output_files.append(elem)
        bar.close()

        return True


class METSBuilder(Task):
    def __init__(self, input_files: List[str], target_prefix: Optional[str] = None):
        self._target: Optional[str] = target_prefix
        if self._target:
            os.makedirs(self._target, exist_ok=True)
        super().__init__(input_files=input_files)

        self._output_files = []
        self._groups: Dict[str, List[str]] = defaultdict(list)
        for file in input_files:
            self._groups[os.path.dirname(file)].append(file)

    def check(self) -> bool:
        """Currently, we rezip everything, simpler that processing zip """
        self._checked_files = {
            file: False
            for file in self.input_files
        }
        return False

    def _process(self, inputs: InputListType) -> bool:
        bar = tqdm.tqdm(
            desc=_sbmsg(f"Processing documents into METS"),
            total=len(self._groups)
        )

        for group in self._groups:
            mets_utils.produce_mets(group, exclude="METS.xml")
            folder = os.path.basename(group)
            mets_name = f"{os.path.join(self._target, folder) or folder}-mets.zip"
            mets_utils.zip_folder(group, mets_name)
            self._output_files.append(mets_name)
            bar.update(1)
        return True
