from dataclasses import dataclass
from typing import List
import pandas as pd

from etl_vouchers.utils import pretty_print


@dataclass
class VoucherStatistic:
    df_orders: pd.DataFrame
    df_barcodes: pd.DataFrame
    df_vouchers: pd.DataFrame

    def show(self):
        self.top_5_buyers()
        self.unused_barcodes()

    def top_5_buyers(self):
        df_resp: pd.DataFrame = (
            self.df_vouchers.groupby(["customer_id"])
            .agg({"order_id": "count"})
            .reset_index()
            .rename(columns={"order_id": "amount_of_tickets"})
            .sort_values(by=["amount_of_tickets"], ascending=False)[:5]
        )

        rows: List[str] = [
            f"{it.customer_id}, {it.amount_of_tickets}" for _, it in df_resp.iterrows()
        ]

        pretty_print(
            "BONUS - Top 5 Customers", ["customer_id, amount_of_tickets", *rows]
        )

    def unused_barcodes(self):
        df_resp: pd.DataFrame = self.df_barcodes.merge(
            self.df_orders, on="order_id", how="left"
        ).pipe(lambda df: df[df["customer_id"].isna()])

        num: int = len(df_resp)
        multi: bool = num > 1

        pretty_print(
            "BONUS - Unused barcodes",
            [
                f"There {'are' if multi else 'is'} {len(df_resp)} unused barcode{'s' if multi else ''}",
                "Barcodes:",
                df_resp["barcode"].to_list(),
            ],
        )
