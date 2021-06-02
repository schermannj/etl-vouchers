from typing import List, Dict
from abc import ABC, abstractmethod
import pandas as pd
import great_expectations as ge
from great_expectations.dataset import PandasDataset
from etl_vouchers.exceptions import InvalidSourceFile
from etl_vouchers.utils import pretty_print


def is_valid(validation_responses: List[Dict]) -> bool:
    return all([it["success"] for it in validation_responses])


class Validator(ABC):
    """
    Base validator class.

    :param df - input pandas DataFrame that has to be validated.
    :param silent - boolean flag, if true - nothing is printed to stdout.
    """

    def __init__(self, df: pd.DataFrame, silent: bool = False):
        self.df: pd.DataFrame = df
        self.dfe: PandasDataset = ge.from_pandas(self.df)
        self.silent: bool = silent

    @abstractmethod
    def __call__(self, **kwargs) -> pd.DataFrame:
        raise NotImplemented


class OrdersValidator(Validator):
    """
    Validator for Orders dataset.
    """

    def __call__(self) -> pd.DataFrame:
        if not self.has_expected_format():
            raise InvalidSourceFile(
                "Orders csv file has to contain 2 columns - customer_id, order_id"
            )

        return self.df

    def has_expected_format(self) -> bool:
        responses: List[Dict] = [
            self.dfe.expect_column_to_exist("customer_id", column_index=0),
            self.dfe.expect_column_values_to_not_be_null(
                "customer_id", catch_exceptions=True
            ),
            self.dfe.expect_column_to_exist("order_id", column_index=1),
            self.dfe.expect_column_values_to_not_be_null(
                "order_id", catch_exceptions=True
            ),
        ]

        return is_valid(responses)


class BarcodesValidator(Validator):
    """
    Validator for barcodes dataset.
    """

    def __call__(self) -> pd.DataFrame:
        if not self.has_expected_format():
            raise InvalidSourceFile(
                "Barcode csv file has to contain 2 columns - barcode, order_id"
            )

        if not self.no_duplicate_barcodes():
            if not self.silent:
                pretty_print(
                    "Barcodes Validator - No Barcode Duplicates",
                    [
                        "Next barcodes are duplicated",
                        *self.df[self.df.duplicated(subset=["barcode"])][
                            "barcode"
                        ].tolist(),
                    ],
                )

            self.df = self.df.drop_duplicates(subset=["barcode"], keep=False)

        if not self.no_orders_without_barcodes():
            if not self.silent:
                pretty_print(
                    "Barcodes Validator - No Orders Without Barcodes",
                    [
                        "Next orders don't have barcodes",
                        *self.df[self.df["barcode"].isna()]["order_id"].tolist(),
                    ],
                )

            self.df = self.df.dropna(subset=["barcode"])

        return self.df

    def has_expected_format(self) -> bool:
        responses: List[Dict] = [
            self.dfe.expect_column_to_exist("barcode", column_index=0),
            self.dfe.expect_column_to_exist("order_id", column_index=1),
        ]

        return is_valid(responses)

    def no_duplicate_barcodes(self) -> bool:
        responses: List[Dict] = [self.dfe.expect_column_values_to_be_unique("barcode")]

        return is_valid(responses)

    def no_orders_without_barcodes(self) -> bool:
        responses: List[Dict] = [
            self.dfe.expect_column_values_to_not_be_null("barcode"),
        ]

        return is_valid(responses)
