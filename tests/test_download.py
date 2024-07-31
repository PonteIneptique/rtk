import pytest

from rtk.task import DownloadIIIFImageTask, DownloadIIIFManifestTask
from tests.utils import register_uri, get_output


def test_download_manifest(requests_mock):
    register_uri(requests_mock, "https://foo.bar/manifest1.json", "manifest1.json")
    register_uri(requests_mock, "https://foo.bar/manifest2.json", "manifest2.json")
    manifests = DownloadIIIFManifestTask(
        ["https://foo.bar/manifest1.json", "https://foo.bar/manifest2.json"],
        output_directory=get_output("manifests")
    )
    manifests.process()
    out_1 = manifests.output_files
    assert len(manifests.output_files) == 5, "The first manifest has 3 images, the second 2"

    manifests = DownloadIIIFManifestTask(
        ["https://foo.bar/manifest1.json", "https://foo.bar/manifest2.json"],
        output_directory=get_output("manifests")
    )
    manifests.process()
    assert len(manifests.output_files) == 5, "The first manifest has 3 images, the second 2"
    assert manifests.output_files == out_1, "Both commands yield the same output"
    assert len(requests_mock.request_history) == 2, "The second process should not recall the same URI"


def test_download_images(requests_mock):
    register_uri(
        requests_mock,
        'https://gallica.bnf.fr/iiif/ark:/12148/bpt6k12401693/f1/full/full/0/native.jpg',
        "page1.jpg",
        binary=True
    )
    register_uri(
        requests_mock,
        'https://gallica.bnf.fr/iiif/ark:/12148/bpt6k12401693/f2/full/full/0/native.jpg',
        "page2.jpg",
        binary=True
    )

    inplist = [
        ('https://gallica.bnf.fr/iiif/ark:/12148/bpt6k12401693/f1/full/full/0/native.jpg', 'e8972d7b51', "f1"),
        ('https://gallica.bnf.fr/iiif/ark:/12148/bpt6k12401693/f2/full/full/0/native.jpg', 'e8972d7b51', "f2")
    ]
    dl = DownloadIIIFImageTask(
        [] + inplist,
        multiprocess=4,
        output_prefix=get_output("")
        # downstream_check=DownloadIIIFImageTask.check_downstream_task("xml", utils.check_content)
    )
    dl.process()
    assert dl.output_files == ['test_output/e8972d7b51/f1.jpg', 'test_output/e8972d7b51/f2.jpg']
    out1 = dl.output_files

    dl = DownloadIIIFImageTask(
        [] + inplist,
        multiprocess=4,
        output_prefix=get_output("")
        # downstream_check=DownloadIIIFImageTask.check_downstream_task("xml", utils.check_content)
    )
    dl.process()
    assert dl.output_files == ['test_output/e8972d7b51/f1.jpg', 'test_output/e8972d7b51/f2.jpg']
    assert len(requests_mock.request_history) == 2, "The second process should not call the same URIs again"
