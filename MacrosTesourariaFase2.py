# %% [markdown]
# Desenvolvido por: Heitor santos
# Empresa: Dataex
# Códificação para tratar as macros do Tesouraria fase 2. Realizando a leitura de arquivos do Google Cloud Storage, tratar as informações e realizar o insert no Google Big Query para seguir com o processo de cargar.

# %% [markdown]
# #Definição dos importes do código.

# %%
import math
import pandas as pd
from numpy.random import seed
from numpy.random import normal
from google.cloud import storage

# %%
def CALC_D1(FWD1, strike1, Vol1, T21):
    D1 = math.log(FWD1 / strike1) + (((math.pow(Vol1, 2)) / 2) * T21) / (Vol1 * math.sqrt(T21))
    return D1

# %%
def CALC_D2(FWD2, strike2, Vol2, T22):
    D2 = math.log(FWD2 / strike2) - ((math.pow(Vol2, 2) / 2) * T22) / (Vol2 * math.sqrt(T22))
    return  D2

# %%
def download_blob(bucket_name, source_blob_name, destination_file_name):
    
    try:
        
        storage_client = storage.Client.from_service_account_json('D:\MeusDocumentos\Clientes\Dataex\BancoABC\dev-terceiros.json')

        bucket = storage_client.bucket(bucket_name)    

        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(str(destination_file_name))


        print(
            "Downloaded storage object {} from bucket {} to local file {}.".format(
                source_blob_name, bucket_name, destination_file_name
            )
        )
    except Exception as e:
        print(e)

# %%
def DERIVATIVOS_OPCAO_IBOV(TIPO, FWD, strike, Vol, PRE, T):
    
    if T > 1:
        
        if TIPO == "0":

            DOI = 0
            
        else:
            
            T2  = T/252
            
            r = math.log(1+PRE)
            
            d1 = CALC_D1(FWD, strike, Vol, T)
    
            d2 = CALC_D2(FWD, strike, Vol, T)
            
            if TIPO == "CALL":
                DOI = math.exp(-r * T2) * (FWD * normal(d1) - strike * normal(d2))
            elif TIPO == "PUT":
                DOI = math.exp(-r * T2) * (FWD * normal(-d1) - strike * normal(-d2))
            else:
                DOI = "ERRO"
        
    else:
        
       	DOI = abs(FWD - strike)
        
        if (TIPO == "CALL" and (FWD - strike) < 0): DOI = 0

        if (TIPO == "PUT" and (strike - FWD) < 0): DOI = 0

        if (TIPO == "0"): DOI = 0
        
    return DOI  

# %%
def FINDINDEXBELOW(A, Value):
    return 2

# %%
def BICUBICINTERPOLATION(TABELA_VOL, delta, T):
    
    return 2
    
print(BICUBICINTERPOLATION())

# %%
def DERIVATIVOS_VOL_OPCAO_IBOV(TIPO, FWD, strike, Vol, PRE, T, TABELA_VOL):
     
    if strike != 0 and T > 1:
            
        T2 = T / 252
        r = math.log(1 + PRE)
            
        VOL2 = Vol + 0.00002
        
        i = 0
        while abs(VOL2 - Vol) > 0.00001:

            Vol = VOL2
            
            d1 = CALC_D1(FWD, strike, Vol, T)
            d2 = CALC_D2(FWD, strike, Vol, T)
            
            delta = math.exp(-r * T2) * normal(d1)
            
            VOL2 = BICUBICINTERPOLATION()
            
            i = i + 1
            if i == 1000:
                exit
            
    
        return VOL2

# %%
def DERIVATIVOS_DELTA_OPCAO_IBOV(TIPO, FWD, strike, Vol, PRE, T):
    
    if T > 1:
        if TIPO == 0:
            DDOI = 0
        
        else:
            
            T2 = T / 252
            r = math.log(1 + PRE)
            
            d1 = CALC_D1(FWD, strike, Vol, T)
            d2 = CALC_D2(FWD, strike, Vol, T)
        
            if TIPO == "CALL":
                DDOI = math.exp(-r * T2) * normal(d1)
            elif TIPO == "PUT":
                DDOI = math.exp(-r * T2) * normal(d1) - 1
            Else: DDOI = "ERRO"

    else:
        DDOI = 0

# %%
def TRATAR_ARQUIVO_VANILLA(read_file):

    try:
        df = pd.read_csv(read_file, encoding='ANSI',  delimiter=';')
        df.rename(columns={"id_src" : "id" , "Cliente_src" : "Cliente" , "Notional_src" : "Notional" , "Call_Put_src" : "Call_Put" , "Strike_src" : "Strike" , "Premio_Tesouraria_BRL_src" : "Premio_Tesouraria_BRL" , "Trade_Date_src" : "Trade_Date" , "Initial_Date_src" : "Initial_Date" , "Fixing_Date_src" : "Fixing_Date" , "Vencimento_src" :"Vencimento" , "Vencimento_Referencia_src" : "Vencimento_Referencia" , "Veiculo_Legal_src" :"Veiculo_Legal" , "Trading_Banking_src" : "Trading_Banking" , "Livro_src" : "Livro" , "Estrategia_src" : "Estrategia" , "Estrategia_Nivel_2_src" : "Estrategia_Nivel_2" , "Liquidacao_Antecipada_x_src" : "Liquidacao_Antecipada_x" , "Data_Liquidacao_src" :"Data_Liquidacao" , "Qtd_Liquid_Vcto_du_src" : "Qtd_Liquid_Vcto_du" , "Premio_Tesouraria_Liq_curva_src" : "Premio_Tesouraria_Liq_curva" , "Taxa_Desconto_src" :"Taxa_Desconto" , "ID_REGISTRO_src" : "ID_REGISTRO", "Data_proc_src" : "Data_Proc" , "Data_base_d0_src" : "Data_base_d0" , "Data_base_d1_src" : "Data_base_d1" , "Data_base_m1_src" : "Data_base_m1" , "Dia_util_d0_src" :"Dia_util_d0" , "Dia_util_d1_src" : "Dia_util_d1" , "Dia_util_m1_src" : "Dia_util_m1" , "Data_src" : "Data_referencia" , "m_trade_date_initial_dc" : "m_trade_date_initial_dc" , "m_trade_date_initial_du" : "m_trade_date_initial_du" , "m_trade_date_fixing_dc" :"m_trade_date_fixing_dc" , "m_trade_date_fixing_du" : "m_trade_date_fixing_du" , "m_trade_date_vencimento_dc" : "m_trade_date_vencimento_dc" , "m_trade_date_vencimento_du" :"m_trade_date_vencimento_du" , "m_data_base_initial_dc" : "m_data_base_initial_dc" , "m_data_base_initial_du" : "m_data_base_initial_du" , "m_data_base_fixing_dc" :"m_data_base_fixing_dc" , "m_data_base_fixing_du" : "m_data_base_fixing_du" , "m_data_base_vencimento_dc" : "m_data_base_vencimento_dc" , "m_data_base_vencimento_du" :"m_data_base_vencimento_du" , "m_taxa_pre_brl_initial_date_perc" :"m_taxa_pre_brl_initial_date_perc" , "m_taxa_pre_brl_fixing_date_perc" : "m_taxa_pre_brl_fixing_date_perc" , "m_taxa_pre_brl_vencimento_perc" :"m_taxa_pre_brl_vencimento_perc" , "m_indice_futuro" : "m_indice_futuro" , "m_vol_perc" :"m_vol_perc" , "m_premio_corrigido" : "m_premio_corrigido" , "m_preco_opcao" : "m_preco_opcao", "pl_ltd_opcao" : "pl_ltd_opcao" , "pl_daily" : "pl_daily" , "pl_month" : "pl_month"}, inplace = True)
        df['Trade_Date']= pd.to_datetime(df['Trade_Date'].str.slice(0, 10), format='%m/%d/%Y')
        df['Initial_Date']= pd.to_datetime(df['Initial_Date'].str.slice(0, 10), format='%m/%d/%Y')
        df['Fixing_Date']= pd.to_datetime(df['Fixing_Date'].str.slice(0, 10), format='%m/%d/%Y')
        df['Vencimento']= pd.to_datetime(df['Vencimento'].str.slice(0, 10), format='%m/%d/%Y')
        #df['Data_Liquidacao']= pd.to_datetime(file['Data_Liquidacao'].str.slice(0, 10), format='%m/%d/%Y')
        
        
        for i in range(len(df)) :

            if ( df["Trade_Date"][i] <  df["Vencimento"][i]) :
                print("verdadeiro")
            else:
                print("falso")

        #df.info()


    except Exception as e:
        
        print(e)

# %% [markdown]
# Definição do nome do Bucket a ser utilizado para conectar no GCP

# %%
v_bucket_name = "abc-pipeline-dev-terceiros_cloudbuild"

# %% [markdown]
# Definição do nome do arquivo com o caminho e sem o bucket a ser buscado para conectar no GCP

# %%
v_source_blob_name = "STAGE_AREA/IBOVOPT_VANILLA.CSV"

# %% [markdown]
# Definição do nome do caminho a ser salvo o arquivo do GCP com o nome a ser utilizado

# %%
v_destination_file_name = "D:\MeusDocumentos\Clientes\Dataex\BancoABC\Tesouraria Fase 02\DEV - Macros\python\TesourariaFase2\IBOVOPT_VANILLA.CSV"

# %%
TRATAR_ARQUIVO_VANILLA(v_destination_file_name)


