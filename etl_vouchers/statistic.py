from dataclasses import dataclass
from typing import List
import pandas as pd

from etl_vouchers.etl import pipeline, extract
from etl_vouchers.utils import pretty_print


@dataclass
class VoucherStatistic:
    """
    Statistic dataclass for vouchers, responsible for data analysis and sending the results to stdout.
    """

    orders_filepath: str
    barcodes_filepath: str

    def top_customers(self, top=5):
        df_vouchers = pipeline(
            self.orders_filepath,
            self.barcodes_filepath,
            transform_only=True,
            silent=True,
        ).df_vouchers

        df_resp: pd.DataFrame = (
            df_vouchers.groupby(["customer_id"])
            .agg({"order_id": "count"})
            .reset_index()
            .rename(columns={"order_id": "amount_of_tickets"})
            .sort_values(by=["amount_of_tickets"], ascending=False)[:top]
        )

        rows: List[str] = [
            f"{it.customer_id}, {it.amount_of_tickets}" for _, it in df_resp.iterrows()
        ]

        pretty_print(
            f"BONUS - Top {top} Customers", ["customer_id, amount_of_tickets", *rows]
        )

    def unused_barcodes(self):
        df_barcodes = extract(self.barcodes_filepath)
        df_resp: pd.DataFrame = df_barcodes[df_barcodes["order_id"].isna()]

        num: int = len(df_resp)
        multi: bool = num > 1

        pretty_print(
            "BONUS - Unused barcodes",
            [
                f"There {'are' if multi else 'is'} {len(df_resp)} unused barcode{'s' if multi or num == 0 else ''}",
                "Barcodes:",
                df_resp["barcode"].to_list(),
            ],
        )
