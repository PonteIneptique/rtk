import pytest
import os
from tests.utils import get_output, clear_output


@pytest.fixture(autouse=True, scope='function')
def remove_output():
    if os.path.exists(get_output("")):
        clear_output()
    yield
    if os.path.exists(get_output("")):
        clear_output()
