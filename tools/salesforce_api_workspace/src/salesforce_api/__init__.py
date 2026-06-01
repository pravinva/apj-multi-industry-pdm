from salesforce_api.functions import salesforce_insert, salesforce_update, salesforce_upsert
from salesforce_api.operators import SalesforceBulkWriteOperator, SalesforceUpsertOperator

__all__ = [
    "salesforce_insert",
    "salesforce_update",
    "salesforce_upsert",
    "SalesforceBulkWriteOperator",
    "SalesforceUpsertOperator",
]
