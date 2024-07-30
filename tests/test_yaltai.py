from rtk.task import YALTAiCommand
from tests.utils import copy_input, get_env_bin, get_input


def test_yaltai(capsys):
    files = copy_input("page*.jpg")
    yaltai = YALTAiCommand(
        files,
        yolo_model=get_input("*.pt")[0],
        multiprocess=1,
        device="cpu",
        raise_on_error=True,
        binary=get_env_bin("yaltaienv", "yaltai")
    )
    yaltai.process()
    assert len(yaltai.output_files) == 2, "Two files were processed"
    assert sorted(yaltai.output_files) == ['test_output/page1.xml', 'test_output/page2.xml'], "Two files were processed"
