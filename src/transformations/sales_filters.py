from pyspark.sql import functions as F

def filter_report_date_range(report_start_date, report_end_date):
    def _transform(df):
        return df.filter(
            (F.col("report_date") >= report_start_date) &
            (F.col("report_date") <= report_end_date)
        )
    return _transform


def filter_market(market):
    def _transform(df):
        if market == "ALL":
            return df
        return df.filter(F.col("market") == market)
    return _transform


def filter_brand(brand):
    def _transform(df):
        if brand == "ALL":
            return df
        return df.filter(F.col("brand") == brand)
    return _transform