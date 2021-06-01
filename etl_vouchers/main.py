import pandas as pd
from etl_vouchers.statistic import VoucherStatistic
from etl_vouchers.utils import current_time, sanitize_path


def _extract_orders(filepath: str):
    return pd.read_csv(filepath)


def _extract_barcodes(filepath: str):
    return pd.read_csv(filepath)


def _transform(
    df_orders: pd.DataFrame,
    df_barcodes: pd.DataFrame,
    ignore_empty_vouchers: bool = False,
):
    df_vouchers: pd.DataFrame = df_orders.merge(
        df_barcodes, on=["order_id"], how="left"
    ).astype({"barcode": pd.Int64Dtype()})

    if ignore_empty_vouchers:
        df_vouchers = df_vouchers.dropna(subset=["barcode"])

    return (
        df_vouchers.sort_values(by=["customer_id", "order_id"])
        .groupby(["customer_id", "order_id"])
        .apply(lambda df_group: df_group["barcode"].dropna().tolist())
        .reset_index()
        .rename(columns={0: "barcodes"})
    )


def _load(df_vouchers: pd.DataFrame, dest_path: str):
    if dest_path is None:
        dest_path = "./"
    else:
        dest_path = sanitize_path(dest_path)

    return df_vouchers.to_csv(f"{dest_path}vouchers.{current_time()}.csv", index=False)


def pipeline(
    orders_filepath: str,
    barcodes_filepath: str,
    ignore_empty_vouchers: bool = False,
    dest_path: str = None,
):
    df_orders: pd.DataFrame = _extract_orders(orders_filepath)
    df_barcodes: pd.DataFrame = _extract_barcodes(barcodes_filepath)

    df_vouchers: pd.DataFrame = _transform(
        df_orders, df_barcodes, ignore_empty_vouchers=ignore_empty_vouchers
    )

    _load(df_vouchers, dest_path)

    VoucherStatistic(df_orders, df_barcodes, df_vouchers).show()


if __name__ == "__main__":
    pipeline(
        "/home/vsheruda/Projects/interview-challenges/etl-vouchers/datasets/orders.1622544686.csv",
        "/home/vsheruda/Projects/interview-challenges/etl-vouchers/datasets/barcodes.1622544683.csv",
        ignore_empty_vouchers=False,
    )
