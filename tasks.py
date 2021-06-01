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
        resp: PipelineResponse = pipeline(orders, barcodes, transform_only=True, silent=True)

        VoucherStatistic(
            resp.df_orders,
            resp.df_barcodes,
            resp.df_vouchers
        ).unused_barcodes()
    except ETLVouchersException as e:
        print("Failed with: ", e)


@task
def top_customers(c, orders, barcodes):
    try:
        resp: PipelineResponse = pipeline(orders, barcodes, transform_only=True, silent=True)

        VoucherStatistic(
            resp.df_orders,
            resp.df_barcodes,
            resp.df_vouchers
        ).top_customers(top=5)
    except ETLVouchersException as e:
        print("Failed with: ", e)
