from motor import executar_conciliacao_empresa

empresa_id = "CGL Contabilidade Galvão LTDA"

path_lancamentos = r"C:\Users\dorag\Downloads\Lancamentos_CGL_2025.xlsx"

df = executar_conciliacao_empresa(
    empresa_id=empresa_id,
    path_lancamentos=path_lancamentos
)

print(df.head())
