from invoke import task
from etl_vouchers.etl import pipeline, PipelineResponse
from etl_vouchers.exceptions import ETLVouchersException
from etl_vouchers.statistic import VoucherStatistic


@task
def etl(c, orders, barcodes, dest=None, allow_useless=False):
    """
    Runs ETL pipeline against the supplied csv files and generates the output file.

    :param c: cmd
    :param orders: path to orders csv
    :param barcodes: path to barcodes csv
    :param dest: path to the output folder
    :param allow_useless: bool, if true, vouchers without any barcodes will be generated as well
    :return:
    """
    if dest is None:
        dest = "./datasets/"

    try:
        resp: PipelineResponse = pipeline(orders, barcodes, dest, allow_useless_vouchers=allow_useless)

        print(f"Output saved to {resp.output_filepath}\n")
    except ETLVouchersException as e:
        print("Failed with: ", e)


@task
def unused_barcodes(c, orders, barcodes):
    """
    Runs statistic class against the supplied data and
    generates output amount of unused barcodes.

    :param c: cmd
    :param orders: path to orders csv
    :param barcodes: path to barcodes csv
    :return: None
    """
    try:
        VoucherStatistic(
            orders_filepath=orders, barcodes_filepath=barcodes
        ).unused_barcodes()
    except ETLVouchersException as e:
        print("Failed with: ", e)


@task
def top_customers(c, orders, barcodes):
    """
    Runs statistic class against the supplied data and
    generates output for top 5 customers that bought the most amount of tickets.

    :param c: cmd
    :param orders: path to orders csv
    :param barcodes: path to barcodes csv
    :return: None
    """
    try:
        VoucherStatistic(
            orders_filepath=orders, barcodes_filepath=barcodes
        ).top_customers(top=5)
    except ETLVouchersException as e:
        print("Failed with: ", e)
