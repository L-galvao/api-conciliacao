from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Request
from fastapi.responses import FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import shutil
import uuid
from dotenv import load_dotenv
import os
import secrets
from datetime import datetime, timedelta
import numpy as np

# =========================================
# CARREGAR VARIÁVEIS DE AMBIENTE
# =========================================

load_dotenv()

# =========================================
# CONFIGURAÇÕES GERAIS
# =========================================

BASE_DIR = Path("data_api")
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EMPRESAS_DIR = Path("data") / "empresas"
EMPRESAS_DIR.mkdir(parents=True, exist_ok=True)

API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise RuntimeError("API_KEY não configurada no ambiente")

# =========================================
# IMPORT DO MOTOR
# =========================================

from motor import executar_conciliacao_empresa

# =========================================
# FASTAPI
# =========================================

app = FastAPI(
    title="API Conciliação Contábil - MVP",
    version="1.1.0"
)

# =========================================
# CORS
# =========================================

FRONTEND_ORIGINS = [
    "http://localhost:8080",
    "https://lovable.app",
    "https://blanchedalmond-grouse-308172.hostingersite.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "https://blanchedalmond-grouse-308172.hostingersite.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

# =========================================
# SEGURANÇA
# =========================================

security = HTTPBearer()

# Tokens temporários (MVP)
TOKENS_TEMP = {}

def validar_token(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    token = credentials.credentials

    if token not in TOKENS_TEMP:
        raise HTTPException(status_code=403, detail="Token inválido")

    if TOKENS_TEMP[token] < datetime.utcnow():
        del TOKENS_TEMP[token]
        raise HTTPException(status_code=403, detail="Token expirado")
    
# =========================================
# ENDPOINT DE OPTIONS
# =========================================

@app.options("/{path:path}")
def options_handler(path: str):
    return {}

# =========================================
# ENDPOINT DE TOKEN
# =========================================

@app.get("/auth/token")
def gerar_token(request: Request):
    origin = request.headers.get("origin")

    if origin not in FRONTEND_ORIGINS:
        raise HTTPException(status_code=403, detail="Origem não autorizada")

    token = secrets.token_urlsafe(32)
    TOKENS_TEMP[token] = datetime.utcnow() + timedelta(minutes=10)

    return {
        "token": token,
        "expires_in_minutes": 10
    }

# =========================================
# HEALTH
# =========================================

@app.get("/health")
def health_check():
    return {"status": "ok"}

# =========================================
# PLANO DE CONTAS
# =========================================

@app.post(
    "/empresas/{empresa_id}/plano-contas",
    dependencies=[Depends(validar_token)]
)
def upload_plano_contas(
    empresa_id: str,
    file: UploadFile = File(...)
):
    if not empresa_id.isdigit():
        raise HTTPException(status_code=400, detail="empresa_id inválido")

    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Arquivo deve ser .xlsx")

    empresa_dir = EMPRESAS_DIR / empresa_id
    empresa_dir.mkdir(parents=True, exist_ok=True)

    plano_path = empresa_dir / "plano_contas.xlsx"
    mapa_path = empresa_dir / "mapa_plano.json"

    if mapa_path.exists():
        raise HTTPException(status_code=409, detail="Plano já mapeado")

    with open(plano_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return {
        "status": "ok",
        "message": "Plano de contas enviado com sucesso",
        "empresa_id": empresa_id
    }

@app.put(
    "/empresas/{empresa_id}/plano-contas",
    dependencies=[Depends(validar_token)]
)
def atualizar_plano_contas(
    empresa_id: str,
    file: UploadFile = File(...)
):
    empresa_dir = EMPRESAS_DIR / empresa_id
    if not empresa_dir.exists():
        raise HTTPException(status_code=404, detail="Empresa não encontrada")

    plano_path = empresa_dir / "plano_contas.xlsx"
    mapa_path = empresa_dir / "mapa_plano.json"

    with open(plano_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    if mapa_path.exists():
        mapa_path.unlink()

    return {
        "status": "ok",
        "message": "Plano de contas atualizado"
    }

# =========================================
# CONCILIAÇÃO
# =========================================

@app.post(
    "/conciliar",
    dependencies=[Depends(validar_token)]
)
def conciliar(
    empresa_id: str,
    request: Request,
    file: UploadFile = File(...),
):
    empresa_dir = EMPRESAS_DIR / empresa_id
    plano_path = empresa_dir / "plano_contas.xlsx"

    if not plano_path.exists():
        raise HTTPException(status_code=409, detail="Plano não encontrado")

    exec_id = str(uuid.uuid4())
    upload_path = UPLOAD_DIR / f"{empresa_id}_{exec_id}.xlsx"

    with open(upload_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    df_resultado, resumo = executar_conciliacao_empresa(
        empresa_id=empresa_id,
        path_lancamentos=upload_path
    )

    accept = request.headers.get("accept") or ""

    if "application/json" in accept:
        df_json = (
            df_resultado
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )

        resumo_json = {}

        for k, v in resumo.items():
            if isinstance(v, dict):
                resumo_json[k] = {
                    sub_k: (0 if (isinstance(sub_v, float) and np.isnan(sub_v)) else sub_v)
                    for sub_k, sub_v in v.items()
                }
            else:
                resumo_json[k] = 0 if (isinstance(v, float) and np.isnan(v)) else v

        return {
            "resumo": resumo_json,
            "dados": df_json.to_dict(orient="records")
        }


    output_path = OUTPUT_DIR / f"resultado_{empresa_id}_{exec_id}.xlsx"
    df_resultado.to_excel(output_path, index=False)

    return FileResponse(
        path=output_path,
        filename=output_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
