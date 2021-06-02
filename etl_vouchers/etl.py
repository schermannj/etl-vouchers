from dataclasses import dataclass
from typing import Optional

import pandas as pd

from etl_vouchers.utils import current_time, sanitize_path
from etl_vouchers.exceptions import InvalidSourceFile
from etl_vouchers.validator import BarcodesValidator, OrdersValidator


def extract(filepath: str) -> pd.DataFrame:
    """
    Extracts data from csv filepath.

    :param filepath: path to a csv file.
    :return: pandas DataFrame that contains data from the supplied csv.
    """
    try:
        return pd.read_csv(filepath)
    except Exception as e:
        raise InvalidSourceFile(f"Can not read file {filepath}: {str(e)}")


def _transform(
    df_orders: pd.DataFrame,
    df_barcodes: pd.DataFrame,
    allow_useless_vouchers: bool = True,
) -> pd.DataFrame:
    """
    Main place responsible for data transformation.

    :param df_orders: pd.DataFrame with orders data
    :param df_barcodes: pd.DataFrame with barcodes data
    :param allow_useless_vouchers: bool, if false all the vouchers without any barcodes will be ignored.
    :return: pd.DataFrame of merged and transformed data
    """
    df_vouchers: pd.DataFrame = df_orders.merge(
        df_barcodes, on=["order_id"], how="left"
    ).astype({"barcode": pd.Int64Dtype()})

    if not allow_useless_vouchers:
        df_vouchers = df_vouchers.dropna(subset=["barcode"])

    return (
        df_vouchers.sort_values(by=["customer_id", "order_id"])
        .groupby(["customer_id", "order_id"])
        .apply(lambda df_group: df_group["barcode"].dropna().tolist())
        .reset_index()
        .rename(columns={0: "barcodes"})
    )


def _load(df_vouchers: pd.DataFrame, dest_path: str) -> str:
    """
    Loads data to a csv file and stores it under dest_path.

    :param df_vouchers: pd.DataFrame with vouchers data.
    :param dest_path: str, path to the desired output folder.
    :return: str, path to the output file.
    """
    if dest_path is None:
        dest_path = "./"
    else:
        dest_path = sanitize_path(dest_path)

    file_path: str = f"{dest_path}vouchers.{current_time()}.csv"

    df_vouchers.to_csv(file_path, index=False)

    return file_path


@dataclass
class PipelineResponse:
    """
    Convenient response dataclass used to aggregate all the results of the pipeline.
    """

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
    allow_useless_vouchers: bool = True,
) -> PipelineResponse:
    """
    Main ETL Pipeline responsible for extraction, transformation and loading of vouchers data.

    :param orders_filepath: filepath to the csv with orders.
    :param barcodes_filepath: filepath to the csv with barcodes data.
    :param dest_path: path under which the output file is stored.
    :param transform_only: bool, if true, the output file will not be generated.
    :param silent: bool, if true, all the output to stdout will be suppressed.
    :param allow_useless_vouchers: bool, if true, vouchers without barcodes will be generated as well.
    :return: PipelineResponse
    """
    df_orders: pd.DataFrame = extract(orders_filepath).pipe(
        lambda df: OrdersValidator(df, silent=silent)()
    )
    df_barcodes: pd.DataFrame = (
        extract(barcodes_filepath)
        .astype({"barcode": pd.Int64Dtype()})
        .pipe(lambda df: BarcodesValidator(df, silent=silent)())
    )

    df_vouchers: pd.DataFrame = _transform(
        df_orders, df_barcodes, allow_useless_vouchers=allow_useless_vouchers
    )

    file_path: Optional[str] = None

    if not transform_only:
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
