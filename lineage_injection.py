from pyapacheatlas.core import AtlasEntity, PurviewClient
from pyapacheatlas.auth import ServicePrincipalAuthentication

auth=ServicePrincipalAuthentication(
    tenant_id="<<YOUR_TENANT_ID>>",
    client_id="<<YOUR_CLIENT_ID>>",
    client_secret="YOUR_CLIENT_SECRET"
)
client = PurviewClient(
    account_name="<<YOUR_PURVIEW_ACCOUNT_NAME>>",
    authentication=auth
)

source=AtlasEntity(
    name="customer.csv",
    typeName="azure_datalake_gen2_path",
    qualified_name="https://<<STORAGE_ACCOUNT>>.dfs.core.windows.net/<<FILE_PATH>>",
    guid="-100"
    )

target = AtlasEntity(
    name="orders.csv",
    typeName="azure_datalake_gen2_path",
    qualified_name="https://<<STORAGE_ACCOUNT>>.dfs.core.windows.net/<<FILE_PATH>>",
    guid="-101",
    attributes={"path": "<<ATTRIBUTES>>"}
)

process = AtlasEntity(
    name="CustomerToOrdersProcess",
    typeName="databricks_process",
    qualified_name="custom.process.customer_to_orders",
    guid="-102",
    attributes={
        "inputs": [{"guid": "-100"}],   # reference to customer.csv
        "outputs": [{"guid": "-101"}]   # reference to orders.csv
    }
)


results = client.upload_entities([source, target, process])
print("Lineage injected:", results)
