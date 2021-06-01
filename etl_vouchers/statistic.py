from typing import List
from dataclasses import dataclass

import pandas as pd


@dataclass
class VoucherStatistic:
    df_orders: pd.DataFrame
    df_barcodes: pd.DataFrame
    df_vouchers: pd.DataFrame

    def show(self):
        self.top_5_buyers()
        self.unused_barcodes()

    def _print(self, header: str, rows: List[str]):
        print("*" * 50)
        print(f"{header}")
        print("*" * 50)

        for it in rows:
            print(it)

        print("*" * 50)
        print("\n" * 2)

    def top_5_buyers(self):
        df_resp: pd.DataFrame = (
            self.df_vouchers.groupby(["customer_id"])
            .agg({"order_id": "count"})
            .reset_index()
            .rename(columns={"order_id": "amount_of_tickets"})
            .sort_values(by=["amount_of_tickets"], ascending=False)[:5]
        )

        rows = [
            f"{it.customer_id}, {it.amount_of_tickets}" for _, it in df_resp.iterrows()
        ]

        self._print("Top 5 Customers", ["customer_id, amount_of_tickets", *rows])

    def unused_barcodes(self):
        df_resp: pd.DataFrame = self.df_barcodes.merge(
            self.df_orders, on="order_id", how="left"
        ).pipe(lambda df: df[df["customer_id"].isna()])

        num: int = len(df_resp)
        multi: bool = num > 1

        self._print(
            "Unused barcodes",
            [
                f"There {'are' if multi else 'is'} {len(df_resp)} unused barcode{'s' if multi else ''}",
                "Barcodes:",
                df_resp["barcode"].to_list(),
            ],
        )
