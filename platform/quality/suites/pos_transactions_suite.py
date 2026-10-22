"""Great Expectations 1.x suite for silver.pos_transactions — built in code so it is reviewable in a PR.

Dùng bởi run_checkpoint.py. Mỗi expectation có lý do nghiệp vụ bên cạnh.
"""

from __future__ import annotations

import great_expectations as gx
import great_expectations.expectations as gxe


def build_suite(name: str = "silver.pos_transactions") -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=name)
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeUnique(column="transaction_id")
    )  # silver đã dedup
    suite.add_expectation(
        gxe.ExpectColumnValuesToNotBeNull(column="store_id")
    )  # null_store đã vào quarantine
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(column="store_id", regex=r"^(HN|SG)\d{2}$")
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="channel", value_set=["store", "app"])
    )
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="total_amount_vnd", min_value=0, max_value=5_000_000
        )
    )  # 5 triệu/hoá đơn là trần hợp lý
    suite.add_expectation(
        gxe.ExpectColumnMeanToBeBetween(
            column="total_amount_vnd", min_value=40_000, max_value=200_000
        )
    )  # ticket trung bình
    suite.add_expectation(
        gxe.ExpectColumnValuesToNotBeNull(column="customer_id", mostly=0.3)
    )  # ≥30% có khách định danh (app luôn có)
    suite.add_expectation(
        gxe.ExpectColumnProportionOfUniqueValuesToBeBetween(
            column="store_id", min_value=0.0, max_value=0.2
        )
    )
    return suite
