from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import shutil
import os
from pathlib import Path
import uuid

# =========================================
# CONFIGURAÇÕES BÁSICAS
# =========================================

API_KEY = "CHAVE_SUPER_SECRETA_123"  # troque isso
BASE_DIR = Path("data_api")
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =========================================
# IMPORT DO SEU MOTOR
# =========================================
# Este import assume que o código da conciliação
# está em um arquivo chamado motor.py
# e contém a função executar_conciliacao_empresa

from motor import executar_conciliacao_empresa


# =========================================
# FASTAPI
# =========================================

app = FastAPI(title="API Conciliação Contábil - MVP")


# =========================================
# ENDPOINT PRINCIPAL
# =========================================

@app.post("/conciliar")
def conciliar(
    api_key: str,
    empresa_id: str,
    file: UploadFile = File(...)
):
    # -----------------------------
    # SEGURANÇA BÁSICA
    # -----------------------------
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="API KEY inválida")

    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Apenas arquivos .xlsx são permitidos")

    # -----------------------------
    # SALVAR ARQUIVO RECEBIDO
    # -----------------------------
    exec_id = str(uuid.uuid4())
    upload_path = UPLOAD_DIR / f"{empresa_id}_{exec_id}.xlsx"

    with open(upload_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # -----------------------------
    # EXECUTAR CONCILIAÇÃO
    # -----------------------------
    try:
        df_resultado = executar_conciliacao_empresa(
            empresa_id=empresa_id,
            path_lancamentos=(upload_path)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar conciliação: {str(e)}")

    # -----------------------------
    # GERAR EXCEL FINAL
    # -----------------------------
    output_path = OUTPUT_DIR / f"resultado_{empresa_id}_{exec_id}.xlsx"
    df_resultado.to_excel(output_path, index=False)

    # -----------------------------
    # DEVOLVER ARQUIVO
    # -----------------------------
    return FileResponse(
        path=output_path,
        filename=output_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
