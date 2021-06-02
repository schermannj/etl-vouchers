class ETLVouchersException(Exception):
    """
    Base system exception.
    """

    pass


class InvalidSourceFile(ETLVouchersException):
    pass
