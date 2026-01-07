import pandas as pd
import numpy as np
from repository import EmpresaRepositoryLocal

# =====================================================
# CONFIGURAÇÕES PADRÃO (PODEM VIR DA API DEPOIS)
# =====================================================
DATE_START = "2025-01-01"
DATE_END   = "2025-12-01"


# =====================================================
# PRÉ-PROCESSAMENTO
# =====================================================

def carregar_base(path):
    return pd.read_excel(path, engine="openpyxl")


def filtrar_periodo(df, date_start, date_end):
    df = df.loc[:, ["Data", "Conta Débito", "Conta Crédito", "Valor", "Descrição Histórico"]].copy()
    df.loc[:, "Data"] = pd.to_datetime(df["Data"])
    return df.loc[
        (df["Data"] >= date_start) &
        (df["Data"] < date_end)
    ].copy()


# =====================================================
# PARTIDA DOBRADA
# =====================================================

def normalizar_partida_dobrada(df):
    df_debito = df.loc[:, ["Data", "Conta Débito", "Valor", "Descrição Histórico"]].copy()
    df_debito.loc[:, "Conta Completa"] = df_debito["Conta Débito"]
    df_debito.loc[:, "D/C"] = "D"
    df_debito.loc[:, "Valor"] = -df_debito["Valor"]

    df_credito = df.loc[:, ["Data", "Conta Crédito", "Valor", "Descrição Histórico"]].copy()
    df_credito.loc[:, "Conta Completa"] = df_credito["Conta Crédito"]
    df_credito.loc[:, "D/C"] = "C"

    return pd.concat(
        [
            df_debito[["Data", "Conta Completa", "D/C", "Valor", "Descrição Histórico"]],
            df_credito[["Data", "Conta Completa", "D/C", "Valor", "Descrição Histórico"]],
        ],
        ignore_index=True
    )


def quebrar_conta(df):
    df = df.copy()
    df.loc[:, "Conta Código"] = df["Conta Completa"].str.extract(r"^(\d+)")
    df.loc[:, "Conta Nome"] = df["Conta Completa"].str.extract(r"-\s*(.*)")
    df.loc[:, "valor_abs"] = df["Valor"].abs().round(2)
    return df


# =====================================================
# DETECÇÃO HIERÁRQUICA DO PLANO DE CONTAS
# =====================================================

def detectar_conta_pai(df_plano, palavras, grupo):
    return df_plano.loc[
        (df_plano["Analítica"] == False) &
        (df_plano["Grupo Conta"] == grupo) &
        (df_plano["Descrição"].str.upper().str.contains("|".join(palavras)))
    ]


def marcar_hierarquia(df_plano, pais, tipo):
    mapa = {}
    for _, pai in pais.iterrows():
        prefixo = str(pai["Conta"])
        filhos = df_plano.loc[
            (df_plano["Conta"].astype(str).str.startswith(prefixo)) &
            (df_plano["Analítica"] == True)
        ]
        for _, f in filhos.iterrows():
            mapa[str(f["Código Reduzido"])] = tipo
    return mapa


def gerar_mapa_plano_contas(df_plano):
    mapa = {}

    mapa.update(marcar_hierarquia(
        df_plano,
        detectar_conta_pai(df_plano, ["CLIENTE", "CONTAS A RECEBER"], 1),
        "CLIENTE"
    ))

    mapa.update(marcar_hierarquia(
        df_plano,
        detectar_conta_pai(df_plano, ["CAIXA", "BANCO", "BANCOS", "DISPONIVEL"], 1),
        "FINANCEIRO"
    ))

    mapa.update(marcar_hierarquia(
        df_plano,
        detectar_conta_pai(df_plano, ["FORNECEDOR", "FORNECEDORES", "CONTAS A PAGAR"], 2),
        "FORNECEDOR"
    ))

    receitas = df_plano.loc[(df_plano["Grupo Conta"] == 3) & (df_plano["Analítica"] == True)]
    for _, r in receitas.iterrows():
        mapa[str(r["Código Reduzido"])] = "RECEITA"

    despesas = df_plano.loc[(df_plano["Grupo Conta"] == 4) & (df_plano["Analítica"] == True)]
    for _, d in despesas.iterrows():
        mapa[str(d["Código Reduzido"])] = "DESPESA"

    patrimonio = df_plano.loc[(df_plano["Grupo Conta"] == 5) & (df_plano["Analítica"] == True)]
    for _, p in patrimonio.iterrows():
        mapa[str(p["Código Reduzido"])] = "PATRIMONIO"

    return mapa


def classificar_contas_por_plano(df, mapa):
    df = df.copy()
    df.loc[:, "tipo_conta"] = df["Conta Código"].astype(str).map(mapa).fillna("OUTRO")
    return df


# =====================================================
# IDENTIFICAÇÃO DE CLIENTE
# =====================================================

def identificar_cliente(df):
    df = df.copy()
    df.loc[:, "Cliente"] = np.where(
        df["tipo_conta"] == "CLIENTE",
        df["Conta Nome"].str.upper().str.strip(),
        pd.NA
    )
    return df


# =====================================================
# CONCILIAÇÃO
# =====================================================

def conciliar_linhas(df):
    df = df.sort_values("Data").reset_index(drop=True).copy()
    df.loc[:, "status_conciliacao"] = "NAO CONCILIADO"
    df.loc[:, "id_conciliacao"] = pd.NA

    conciliacao_id = 1

    for idx, row in df.iterrows():
        if df.at[idx, "status_conciliacao"] == "CONCILIADO":
            continue

        valor = row["valor_abs"]
        cliente = row["Cliente"]
        data = row["Data"]
        tipo = row["tipo_conta"]
        dc = row["D/C"]

        if tipo == "RECEITA" and dc == "C":
            candidatos = df.loc[
                (df.index != idx) &
                (df["tipo_conta"] == "FINANCEIRO") &
                (df["D/C"] == "D") &
                (df["valor_abs"] == valor) &
                (df["Cliente"] == cliente) &
                (df["Data"] >= data) &
                (df["status_conciliacao"] == "NAO CONCILIADO")
            ]

        elif tipo == "CLIENTE" and dc == "D":
            candidatos = df.loc[
                (df.index != idx) &
                (df["tipo_conta"] == "CLIENTE") &
                (df["D/C"] == "C") &
                (df["valor_abs"] == valor) &
                (df["Cliente"] == cliente) &
                (df["Data"] >= data) &
                (df["status_conciliacao"] == "NAO CONCILIADO")
            ]
        else:
            continue

        if not candidatos.empty:
            idx_par = candidatos.index[0]
            df.at[idx, "status_conciliacao"] = "CONCILIADO"
            df.at[idx_par, "status_conciliacao"] = "CONCILIADO"
            df.at[idx, "id_conciliacao"] = conciliacao_id
            df.at[idx_par, "id_conciliacao"] = conciliacao_id
            conciliacao_id += 1

    return df


# =====================================================
# STATUS FINAL
# =====================================================

def classificar_status(df):
    df = df.copy()

    def definir(row):
        if row["status_conciliacao"] == "CONCILIADO":
            return "CONCILIADO"
        if row["tipo_conta"] == "RECEITA" and row["D/C"] == "C":
            return "NF EM ABERTO"
        if row["tipo_conta"] == "CLIENTE" and row["D/C"] == "D":
            return "NF EM ABERTO"
        if row["tipo_conta"] == "CLIENTE" and row["D/C"] == "C":
            return "RECEBIDO SEM NF"
        if row["tipo_conta"] == "FINANCEIRO" and row["D/C"] == "D":
            return "RECEBIDO SEM NF"
        return "OUTRO"

    df.loc[:, "status_conciliacao"] = df.apply(definir, axis=1)
    return df


# =====================================================
# PIPELINE FINAL (USANDO REPOSITORY)
# =====================================================

def executar_conciliacao_empresa(
    empresa_id: str,
    path_lancamentos: str,
    date_start: str = DATE_START,
    date_end: str = DATE_END
):
    repo = EmpresaRepositoryLocal()

    # -------- Lançamentos --------
    df = carregar_base(path_lancamentos)
    df = filtrar_periodo(df, date_start, date_end)
    df = normalizar_partida_dobrada(df)
    df = quebrar_conta(df)

    # -------- Plano de contas / Mapa --------
    mapa = repo.carregar_mapa_plano(empresa_id)

    if mapa is None:
        df_plano = repo.carregar_plano_contas(empresa_id)
        if df_plano is None:
            raise ValueError(f"Plano de contas não encontrado para empresa '{empresa_id}'")

        mapa = gerar_mapa_plano_contas(df_plano)
        repo.salvar_mapa_plano(empresa_id, mapa)

    # -------- Classificação / Conciliação --------
    df = classificar_contas_por_plano(df, mapa)
    df = identificar_cliente(df)
    df = conciliar_linhas(df)
    df = classificar_status(df)

    return df[
        ["Data", "Cliente", "Conta Código", "Conta Nome", "D/C",
         "tipo_conta", "status_conciliacao", "Valor", "Descrição Histórico"]
    ]
