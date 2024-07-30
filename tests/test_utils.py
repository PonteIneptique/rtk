from rtk.utils import pdf_name_scheme

def test_pdf_name_scheme():
    assert pdf_name_scheme("check.pdf") == 'check/f{}.jpg'
    assert pdf_name_scheme("blop/check.pdf") == 'blop/check/f{}.jpg'
    assert pdf_name_scheme("check.pdf", output_dir="output") == 'output/check/f{}.jpg'
    assert pdf_name_scheme("blop/check.pdf", output_dir="output") == 'output/check/f{}.jpg'
    assert pdf_name_scheme("check.pdf", page_prefix='p') == 'check/p{}.jpg'
