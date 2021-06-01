from dataclasses import dataclass
from typing import Optional

import pandas as pd

from etl_vouchers.utils import current_time, sanitize_path
from etl_vouchers.exceptions import InvalidSourceFile
from etl_vouchers.validator import BarcodesValidator, OrdersValidator


def extract(filepath: str):
    try:
        return pd.read_csv(filepath)
    except Exception as e:
        raise InvalidSourceFile(f"Can not read file {filepath}: {str(e)}")


def _transform(
    df_orders: pd.DataFrame,
    df_barcodes: pd.DataFrame,
):
    df_vouchers: pd.DataFrame = df_orders.merge(
        df_barcodes, on=["order_id"], how="left"
    ).astype({"barcode": pd.Int64Dtype()})

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

    file_path = f"{dest_path}vouchers.{current_time()}.csv"

    df_vouchers.to_csv(file_path, index=False)

    return file_path


@dataclass
class PipelineResponse:
    df_orders: pd.DataFrame
    df_barcodes: pd.DataFrame
    df_vouchers: pd.DataFrame
    output_filepath: Optional[str]


def pipeline(
    orders_filepath: str,
    barcodes_filepath: str,
    dest_path: str = None,
    transform_only: bool = False,
    silent: bool = False,
):
    df_orders: pd.DataFrame = extract(orders_filepath).pipe(
        lambda df: OrdersValidator(df, silent=silent)()
    )
    df_barcodes: pd.DataFrame = extract(barcodes_filepath).pipe(
        lambda df: BarcodesValidator(df, silent=silent)()
    )

    df_vouchers: pd.DataFrame = _transform(df_orders, df_barcodes)

    if transform_only:
        file_path = None
    else:
        file_path = _load(df_vouchers, dest_path)

    return PipelineResponse(
        df_orders=df_orders,
        df_barcodes=df_barcodes,
        df_vouchers=df_vouchers,
        output_filepath=file_path,
    )


if __name__ == "__main__":
    pipeline(
        "../datasets/orders.1622544686.csv",
        "../datasets/barcodes.1622544683.csv",
    )
