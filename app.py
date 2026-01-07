from fastapi import FastAPI, UploadFile, File, HTTPException, Header, Depends
from fastapi.responses import FileResponse
from pathlib import Path
import shutil
import uuid
from dotenv import load_dotenv
load_dotenv()
import os

# =========================================
# CONFIGURAÇÕES
# =========================================

BASE_DIR = Path("data_api")
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise RuntimeError("API_KEY não configurada no ambiente")

# =========================================
# IMPORT DO MOTOR DE CONCILIAÇÃO
# =========================================

from motor import executar_conciliacao_empresa

# =========================================
# FASTAPI
# =========================================

app = FastAPI(
    title="API Conciliação Contábil - MVP",
    version="1.0.0"
)

# =========================================
# DEPENDÊNCIA DE SEGURANÇA
# =========================================

def validar_api_key(authorization: str = Header(...)):
    """
    Espera header:
    Authorization: Bearer SUA_API_KEY
    """
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=403, detail="API KEY inválida")

# =========================================
# ENDPOINT DE SAÚDE
# =========================================

@app.get("/health")
def health_check():
    return {"status": "ok"}

# =========================================
# ENDPOINT PRINCIPAL
# =========================================

@app.post("/conciliar", dependencies=[Depends(validar_api_key)])
def conciliar(
    empresa_id: str,
    file: UploadFile = File(...)
):
    # -----------------------------
    # VALIDAÇÕES
    # -----------------------------
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(
            status_code=400,
            detail="Apenas arquivos .xlsx são permitidos"
        )

    # -----------------------------
    # SALVAR ARQUIVO RECEBIDO
    # -----------------------------
    exec_id = str(uuid.uuid4())
    upload_path = UPLOAD_DIR / f"{empresa_id}_{exec_id}.xlsx"

    try:
        with open(upload_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception:
        raise HTTPException(status_code=500, detail="Erro ao salvar arquivo")

    # -----------------------------
    # EXECUTAR CONCILIAÇÃO
    # -----------------------------
    try:
        df_resultado = executar_conciliacao_empresa(
            empresa_id=empresa_id,
            path_lancamentos=upload_path
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar conciliação: {str(e)}"
        )

    # -----------------------------
    # GERAR EXCEL FINAL
    # -----------------------------
    output_path = OUTPUT_DIR / f"resultado_{empresa_id}_{exec_id}.xlsx"

    try:
        df_resultado.to_excel(output_path, index=False)
    except Exception:
        raise HTTPException(status_code=500, detail="Erro ao gerar arquivo final")

    # -----------------------------
    # DEVOLVER ARQUIVO
    # -----------------------------
    return FileResponse(
        path=output_path,
        filename=output_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
