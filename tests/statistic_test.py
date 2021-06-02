import pytest
import pandas as pd
from etl_vouchers.statistic import VoucherStatistic


class TestVoucherStatistic:
    @pytest.fixture(autouse=True)
    def mock_etl(self, mocker):
        pipeline_response_mock = mocker.MagicMock(
            df_vouchers=pd.DataFrame(
                {
                    "customer_id": [1, 2, 3, 1, 2, 4, 1, 3, 1],
                    "order_id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                }
            )
        )
        mocker.patch(
            "etl_vouchers.statistic.pipeline", return_value=pipeline_response_mock
        )
        mocker.patch(
            "etl_vouchers.statistic.extract",
            return_value=pd.DataFrame(
                {"barcode": [1, 2, 3, 4, 5], "order_id": [None, 2, None, 4, None]}
            ),
        )

    def test_top_5_customers(self):
        assert (
            VoucherStatistic("test", "test")
            .top_customers(2)
            .equals(pd.DataFrame({"customer_id": [1, 2], "amount_of_tickets": [4, 2]}))
        )

    def test_unused_barcodes(self):
        assert VoucherStatistic("test", "test").unused_barcodes()[
            "barcode"
        ].tolist() == [1, 3, 5]
