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
    def __init__(self, df):
        self.df: pd.DataFrame = df
        self.dfe: PandasDataset = ge.from_pandas(self.df)

    @abstractmethod
    def __call__(self, **kwargs) -> pd.DataFrame:
        raise NotImplemented


class OrdersValidator(Validator):
    def __call__(self) -> pd.DataFrame:
        self._has_expected_format()

        return self.df

    def _has_expected_format(self):
        responses: List[Dict] = [
            self.dfe.expect_column_to_exist("customer_id", column_index=0),
            self.dfe.expect_column_values_to_not_be_null("customer_id"),
            self.dfe.expect_column_to_exist("order_id", column_index=1),
            self.dfe.expect_column_values_to_not_be_null("order_id"),
        ]

        if not is_valid(responses):
            raise InvalidSourceFile(
                "Orders csv file has to contain 2 columns - customer_id, order_id"
            )


class BarcodesValidator(Validator):
    def __call__(self, ignore_unused_barcodes=True) -> pd.DataFrame:
        self._has_expected_format()
        self._no_duplicate_barcodes()

        if ignore_unused_barcodes:
            self._no_orders_without_barcodes()

        return self.df

    def _has_expected_format(self):
        responses: List[Dict] = [
            self.dfe.expect_column_to_exist("barcode", column_index=0),
            self.dfe.expect_column_to_exist("order_id", column_index=1),
        ]

        if not is_valid(responses):
            raise InvalidSourceFile(
                "Barcode csv file has to contain 2 columns - barcode, order_id"
            )

    def _no_duplicate_barcodes(self):
        responses: List[Dict] = [self.dfe.expect_column_values_to_be_unique("barcode")]

        if not is_valid(responses):
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

    def _no_orders_without_barcodes(self):
        responses: List[Dict] = [
            self.dfe.expect_column_values_to_not_be_null("order_id"),
        ]

        if not is_valid(responses):
            pretty_print(
                "Barcodes Validator - No Orders Without Barcodes",
                [
                    "Next orders don't have barcodes",
                    *self.df[self.df["order_id"].isna()]["barcode"].tolist(),
                ],
            )

            self.df = self.df.dropna(subset=["order_id"])
