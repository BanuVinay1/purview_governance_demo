from pyapacheatlas.core.client import PurviewClient
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core.entity import AtlasEntity

auth=ServicePrincipalAuthentication(
    tenant_id="<<YOUR_TENANT_ID>>",
    client_id="<<YOUR_CLIENT_ID>>",
    client_secret="YOUR_CLIENT_SECRET"
)
client = PurviewClient(
    account_name="<<YOUR_PURVIEW_ACCOUNT_NAME>>",
    authentication=auth
)

source = AtlasEntity(
    name="CustomerCSV",
    typeName="azure_datalake_gen2_path",
    qualified_name="https://<<STORAGE_ACCOUNT>>.dfs.core.windows.net/<<FILE_PATH>>",
    guid="-101",
    attributes={"path": "/demo/customer.csv", "sourceSystem": "CRM"}
)

target = AtlasEntity(
    name="OrdersCSV",
    typeName="azure_datalake_gen2_path",
    qualified_name="https://<<STORAGE_ACCOUNT>>.dfs.core.windows.net/<<FILE_PATH>>",
    guid="-102",
    attributes={"path": "/demo/orders.csv", "sourceSystem": "ERP"}
)

process = AtlasEntity(
    name="CustomerToOrdersProcess-entity",
    typeName="databricks_process",
    qualified_name="custom.process.customer_to_orders",
    guid="-103",
    attributes={
        "processType": "ETL Job",         # custom attribute
        "inputs": [{"guid": "-101"}],     # reference to CustomerCSV
        "outputs": [{"guid": "-102"}]     # reference to OrdersCSV
    }
)

# Bulk upload
results = client.upload_entities([source, target, process])
print("Bulk upload result:", results)