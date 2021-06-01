from invoke import task
from etl_vouchers.etl import pipeline, PipelineResponse
from etl_vouchers.exceptions import ETLVouchersException
from etl_vouchers.statistic import VoucherStatistic


@task
def etl(c, orders, barcodes, dest=None):
    if dest is None:
        dest = "./datasets/"

    try:
        resp: PipelineResponse = pipeline(orders, barcodes, dest)

        print(f"Output saved to {resp.output_filepath}")
    except ETLVouchersException as e:
        print("Failed with: ", e)


@task
def unused_barcodes(c, orders, barcodes):
    try:
        VoucherStatistic(
            orders_filepath=orders, barcodes_filepath=barcodes
        ).unused_barcodes()
    except ETLVouchersException as e:
        print("Failed with: ", e)


@task
def top_customers(c, orders, barcodes):
    try:
        VoucherStatistic(
            orders_filepath=orders, barcodes_filepath=barcodes
        ).top_customers(top=5)
    except ETLVouchersException as e:
        print("Failed with: ", e)
