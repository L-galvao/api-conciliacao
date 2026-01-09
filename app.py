from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
from pathlib import Path
import shutil
import uuid
from dotenv import load_dotenv
import os

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

# Diretório das empresas
EMPRESAS_DIR = Path("data") / "empresas"
EMPRESAS_DIR.mkdir(parents=True, exist_ok=True)

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
# HABILITAR CORS
# =========================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "https://lovable.app",
        "https://blanchedalmond-grouse-308172.hostingersite.com",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "OPTIONS"],
    allow_headers=[
        "Authorization",
        "Content-Type",
        "Accept",
    ],
)

# =========================================
# SEGURANÇA (HTTP BEARER)
# =========================================

security = HTTPBearer()

def validar_api_key(
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    token = credentials.credentials
    if token != API_KEY:
        raise HTTPException(status_code=403, detail="API KEY inválida")

# =========================================
# ENDPOINT DE SAÚDE
# =========================================

@app.get("/health")
def health_check():
    return {"status": "ok"}

# =========================================
# POST: PRIMEIRO UPLOAD DO PLANO DE CONTAS
# =========================================

@app.post(
    "/empresas/{empresa_id}/plano-contas",
    dependencies=[Depends(validar_api_key)]
)
def upload_plano_contas(
    empresa_id: str,
    file: UploadFile = File(...)
):
    if not empresa_id.isdigit():
        raise HTTPException(
            status_code=400,
            detail="empresa_id deve ser o CNPJ sem pontuação"
        )

    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(
            status_code=400,
            detail="Plano de contas deve ser um arquivo .xlsx"
        )

    empresa_dir = EMPRESAS_DIR / empresa_id
    empresa_dir.mkdir(parents=True, exist_ok=True)

    resultados_dir = empresa_dir / "resultados"
    resultados_dir.mkdir(exist_ok=True)

    plano_path = empresa_dir / "plano_contas.xlsx"
    mapa_path = empresa_dir / "mapa_plano.json"

    # REGRA ATUAL MANTIDA
    if mapa_path.exists():
        raise HTTPException(
            status_code=409,
            detail="Plano de contas já mapeado. Use PUT para atualizar."
        )

    try:
        with open(plano_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Erro ao salvar plano de contas"
        )

    return {
        "status": "ok",
        "message": "Plano de contas enviado com sucesso. Agora o mapa pode ser gerado.",
        "empresa_id": empresa_id
    }

# =========================================
# PUT: ATUALIZAÇÃO DO PLANO DE CONTAS
# =========================================

@app.put(
    "/empresas/{empresa_id}/plano-contas",
    dependencies=[Depends(validar_api_key)]
)
def atualizar_plano_contas(
    empresa_id: str,
    file: UploadFile = File(...)
):
    """
    Atualiza o plano de contas de uma empresa já existente.
    Sobrescreve plano_contas.xlsx e invalida o mapa_plano.json,
    forçando a regeneração automática na próxima conciliação.
    """

    if not empresa_id.isdigit():
        raise HTTPException(
            status_code=400,
            detail="empresa_id deve ser o CNPJ sem pontuação"
        )

    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(
            status_code=400,
            detail="Plano de contas deve ser um arquivo .xlsx"
        )

    empresa_dir = EMPRESAS_DIR / empresa_id

    if not empresa_dir.exists():
        raise HTTPException(
            status_code=404,
            detail="Empresa não encontrada. Use POST para cadastrar o plano."
        )

    plano_path = empresa_dir / "plano_contas.xlsx"
    mapa_path = empresa_dir / "mapa_plano.json"

    try:
        with open(plano_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Erro ao atualizar plano de contas"
        )

    # INVALIDA O MAPA ANTIGO (regra crítica e correta)
    if mapa_path.exists():
        mapa_path.unlink()

    return {
        "status": "ok",
        "message": "Plano de contas atualizado com sucesso. O mapa será regenerado automaticamente na próxima conciliação.",
        "empresa_id": empresa_id
    }

# =========================================
# ENDPOINT PRINCIPAL: CONCILIAÇÃO
# =========================================

@app.post("/conciliar", dependencies=[Depends(validar_api_key)])
def conciliar(
    empresa_id: str,
    file: UploadFile = File(...),
    request: Request = None
):
    if not empresa_id.isdigit():
        raise HTTPException(
            status_code=400,
            detail="empresa_id deve ser o CNPJ sem pontuação"
        )

    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(
            status_code=400,
            detail="Apenas arquivos .xlsx são permitidos"
        )

    empresa_dir = EMPRESAS_DIR / empresa_id
    plano_path = empresa_dir / "plano_contas.xlsx"

    if not plano_path.exists():
        raise HTTPException(
            status_code=409,
            detail="Plano de contas não mapeado. Envie ou atualize o plano antes de conciliar."
        )

    exec_id = str(uuid.uuid4())
    upload_path = UPLOAD_DIR / f"{empresa_id}_{exec_id}.xlsx"

    try:
        with open(upload_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception:
        raise HTTPException(status_code=500, detail="Erro ao salvar arquivo")

    try:
        df_resultado, resumo = executar_conciliacao_empresa(
            empresa_id=empresa_id,
            path_lancamentos=upload_path
        )

        accept = request.headers.get("accept", "")

        if "application/json" in accept:
            return {
                "resumo": resumo,
                "dados": df_resultado.to_dict(orient="records")
            }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar conciliação: {str(e)}"
        )

    output_path = OUTPUT_DIR / f"resultado_{empresa_id}_{exec_id}.xlsx"

    try:
        df_resultado.to_excel(output_path, index=False)
    except Exception:
        raise HTTPException(status_code=500, detail="Erro ao gerar arquivo final")

    return FileResponse(
        path=output_path,
        filename=output_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
