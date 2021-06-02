import pytest
from etl_vouchers.utils import current_time, sanitize_path


@pytest.mark.parametrize(
    "time_resp, expected_response,", [(1.1, 1), (1.111, 1), (1, 1), (10, 10)]
)
def test_current_time(mocker, time_resp, expected_response):
    time_mock = mocker.MagicMock()
    time_mock.time = 4
    mocker.patch("etl_vouchers.utils.time.time", return_value=time_resp)

    assert current_time() == expected_response


@pytest.mark.parametrize(
    "input_path, expected_path",
    [
        ("./test/path", "./test/path/"),
        ("./test/path/", "./test/path/"),
        ("./", "./"),
        (".", "./"),
    ],
)
def test_sanitize_time(input_path, expected_path):
    assert sanitize_path(input_path) == expected_path
