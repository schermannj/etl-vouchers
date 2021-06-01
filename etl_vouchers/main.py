from dataclasses import dataclass

import pandas as pd
import time


def _extract_orders(filepath: str):
    return pd.read_csv(filepath)


def _extract_barcodes(filepath: str):
    return pd.read_csv(filepath)


def _transform(df_orders: pd.DataFrame, df_barcodes: pd.DataFrame, ignore_empty_vouchers: bool = False):
    df_vouchers: pd.DataFrame = (
        df_orders.merge(df_barcodes, on=["order_id"], how="left")
            .astype({"barcode": pd.Int64Dtype()})
    )

    if ignore_empty_vouchers:
        df_vouchers = df_vouchers.dropna(subset=["barcode"])

    return (
        df_vouchers
            .sort_values(by=["customer_id", "order_id"])
            .groupby(["customer_id", "order_id"])
            .apply(lambda df_group: df_group["barcode"].dropna().tolist())
            .reset_index()
            .rename(columns={0: "barcodes"})
    )


def current_time_millis():
    return round(time.time())


def _load(df_vouchers: pd.DataFrame, dest_path: str):
    if dest_path is None:
        dest_path = "./"
    elif not dest_path.endswith("/"):
        dest_path += "/"

    return df_vouchers.to_csv(f"{dest_path}vouchers.{round(time.time())}.csv", index=False)


@dataclass
class VoucherStatistic:
    df_orders: pd.DataFrame
    df_barcodes: pd.DataFrame
    df_vouchers: pd.DataFrame

    def _print(self, header, rows):
        print("*" * 50)
        print(f"{header}")
        print("*" * 50)

        for it in rows:
            print(it)

        print("*" * 50)
        print("\n" * 2)

    def top_5_buyers(self):
        df_resp = (
            self.df_vouchers.groupby(["customer_id"])
            .agg({"order_id": "count"})
            .reset_index()
            .rename(columns={"order_id": "amount_of_tickets"})
            .sort_values(by=["amount_of_tickets"], ascending=False)
            [:5]
        )

        rows = [
            f"{it.customer_id}, {it.amount_of_tickets}"
            for _, it
            in df_resp.iterrows()
        ]

        self._print(
            "Top 5 Customers",
            [
                "customer_id, amount_of_tickets",
                *rows
            ]
        )

    def unused_barcodes(self):
        df_resp: pd.DataFrame = (
            self.df_barcodes.merge(self.df_orders, on="order_id", how="left")
            .pipe(
                lambda df: df[df["customer_id"].isna()]
            )
        )

        num: int = len(df_resp)
        multi: bool = num > 1

        self._print(
            "Unused barcodes",
            [
                f"There {'are' if multi else 'is' } {len(df_resp)} unused barcode{'s' if multi else ''}",
                "Barcodes:",
                df_resp["barcode"].to_list()
            ]
        )


def generate_vouchers(
        orders_filepath: str,
        barcodes_filepath: str,
        ignore_empty_vouchers: bool = False,
        dest_path: str = None
):
    df_orders: pd.DataFrame = _extract_orders(orders_filepath)
    df_barcodes: pd.DataFrame = _extract_barcodes(barcodes_filepath)

    df_vouchers: pd.DataFrame = _transform(df_orders, df_barcodes, ignore_empty_vouchers=ignore_empty_vouchers)

    _load(df_vouchers, dest_path)

    # TODO: refactor this piece
    vs = VoucherStatistic(
        df_orders,
        df_barcodes,
        df_vouchers
    )
    vs.top_5_buyers()
    vs.unused_barcodes()

    return df_vouchers


if __name__ == "__main__":
    generate_vouchers(
        "/home/vsheruda/Projects/interview-challenges/etl-vouchers/datasets/orders.1622544686.csv",
        "/home/vsheruda/Projects/interview-challenges/etl-vouchers/datasets/barcodes.1622544683.csv",
        ignore_empty_vouchers=False
    )
