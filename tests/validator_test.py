import pandas as pd
import pytest
from etl_vouchers.validator import OrdersValidator, BarcodesValidator, is_valid


@pytest.mark.parametrize(
    "validation_response, is_valid_response",
    [
        (
            [
                {"success": True},
                {"success": False},
            ],
            False,
        ),
        (
            [
                {"success": False},
                {"success": False},
            ],
            False,
        ),
        (
            [
                {"success": False},
                {"success": True},
            ],
            False,
        ),
        (
            [
                {"success": True},
                {"success": True},
            ],
            True,
        ),
    ],
)
def test_is_valid(validation_response, is_valid_response):
    assert is_valid(validation_response) == is_valid_response


class TestOrdersValidator:
    @pytest.mark.parametrize(
        "input_df, is_valid_response",
        [
            (pd.DataFrame({"customer_ids": [], "order_id": []}), False),
            (pd.DataFrame({"customer_id": [], "order_ids": []}), False),
            (pd.DataFrame({"customer_id": [1], "order_id": [None]}), False),
            (pd.DataFrame({"customer_id": [None], "order_id": [1]}), False),
            (pd.DataFrame({"customer_id": [1], "order_id": [1]}), True),
        ],
    )
    def test_has_expected_format(self, input_df, is_valid_response):
        validator = OrdersValidator(input_df, silent=True)

        assert validator.has_expected_format() == is_valid_response


class TestBarcodesValidator:
    @pytest.mark.parametrize(
        "input_df, is_valid_response",
        [
            (pd.DataFrame({"barcodes": [], "order_id": []}), False),
            (pd.DataFrame({"barcode": [], "order_ids": []}), False),
            (pd.DataFrame({"barcode": [1], "order_id": [1]}), True),
        ],
    )
    def test_has_expected_format(self, input_df, is_valid_response):
        validator = BarcodesValidator(input_df, silent=True)

        assert validator.has_expected_format() == is_valid_response

    @pytest.mark.parametrize(
        "input_df, is_valid_response",
        [
            (pd.DataFrame({"barcode": [1, 1], "order_id": [1, 2]}), False),
            (pd.DataFrame({"barcode": [1, 2], "order_id": [1, 2]}), True),
        ],
    )
    def test_no_duplicate_barcodes(self, input_df, is_valid_response):
        validator = BarcodesValidator(input_df, silent=True)

        assert validator.no_duplicate_barcodes() == is_valid_response

    @pytest.mark.parametrize(
        "input_df, is_valid_response",
        [
            (pd.DataFrame({"barcode": [None, 1], "order_id": [1, 2]}), False),
            (pd.DataFrame({"barcode": [1, 2], "order_id": [1, 2]}), True),
        ],
    )
    def test_no_orders_without_barcodes(self, input_df, is_valid_response):
        validator = BarcodesValidator(input_df, silent=True)

        assert validator.no_orders_without_barcodes() == is_valid_response
