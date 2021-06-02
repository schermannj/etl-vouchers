import pandas as pd
import pytest
from etl_vouchers import etl


def _extract_mock(df_orders, df_barcodes):
    def _inner_mock(filepath):
        if filepath == "orders":
            return df_orders
        return df_barcodes

    return _inner_mock


@pytest.mark.parametrize(
    "df_orders, df_barcodes, df_expected_vouchers, allow_useless",
    [
        (
            pd.DataFrame(
                {
                    "customer_id": [
                        1,
                        1,
                        1,
                        1,
                        2,
                        2,
                        2,
                        3,
                        3,
                        3,
                        3,
                        4,
                        4,
                        5,
                        6,
                        7,
                        8,
                    ],
                    "order_id": [
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        10,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                    ],
                }
            ),
            pd.DataFrame(
                {
                    "barcode": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, None, 7, 8, 10],
                    "order_id": [1, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                }
            ),
            pd.DataFrame(
                {
                    "customer_id": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 5, 6, 7, 8],
                    "order_id": [
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        10,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                    ],
                    "barcodes": [
                        [1, 2],
                        [3],
                        [4],
                        [5, 6],
                        [],
                        [],
                        [9],
                        [],
                        [11],
                        [12],
                        [],
                        [],
                        [],
                        [],
                        [],
                        [],
                        [],
                    ],
                }
            ),
            True,
        ),
        (
            pd.DataFrame(
                {
                    "customer_id": [
                        1,
                        1,
                        1,
                        1,
                        2,
                        2,
                        2,
                        3,
                        3,
                        3,
                        3,
                        4,
                        4,
                        5,
                        6,
                        7,
                        8,
                    ],
                    "order_id": [
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        10,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                    ],
                }
            ),
            pd.DataFrame(
                {
                    "barcode": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, None, 7, 8, 10],
                    "order_id": [1, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                }
            ),
            pd.DataFrame(
                {
                    "customer_id": [1, 1, 1, 1, 2, 3, 3],
                    "order_id": [1, 2, 3, 4, 7, 9, 10],
                    "barcodes": [[1, 2], [3], [4], [5, 6], [9], [11], [12]],
                }
            ),
            False,
        ),
    ],
)
def test_etl(mocker, df_orders, df_barcodes, df_expected_vouchers, allow_useless):
    mocker.patch("etl_vouchers.etl.extract")
    etl.extract = _extract_mock(df_orders, df_barcodes)

    mocker.patch("etl_vouchers.etl._load", return_value="./test/output/path.csv")

    resp = etl.pipeline("orders", "barcodes", allow_useless_vouchers=allow_useless)

    assert resp is not None
    assert resp.df_orders is not None
    assert resp.df_barcodes is not None
    assert resp.df_vouchers.equals(df_expected_vouchers)
    assert resp.output_filepath == "./test/output/path.csv"

    etl._load.assert_called_once()
