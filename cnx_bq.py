# %%
# Python
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account

# %%
key_path = "D:\MeusDocumentos\Clientes\Dataex\BancoABC\dev-terceiros.json"

credentials = service_account.Credentials.from_service_account_file(
     key_path,
     scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(
     credentials=credentials,
     project=credentials.project_id
)

# %%
query = """
    SELECT Data_ref, Data, Delta_50 FROM `abc-pipeline-dev-terceiros.DS_RISCOS_RESULTADOS.STG_TB_VOL_IND` LIMIT 10 
    """

# %%
rows = (
    client.query(query)
    .result()
    .to_dataframe()
)


# %%
#for row in rows:
#    print(row)

# %%
df = pd.DataFrame(rows)

print(df.iloc[0,0])


