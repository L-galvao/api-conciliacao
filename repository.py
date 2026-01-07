import json
from pathlib import Path
import pandas as pd


class EmpresaRepositoryLocal:
    """
    Repositório local por empresa.
    Hoje usa arquivos.
    Amanhã pode virar Postgres / S3 sem mudar o motor.
    """

    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)

    # =====================================================
    # PATHS
    # =====================================================

    def _empresa_dir(self, empresa_id: str) -> Path:
        path = self.base_dir / "empresas" / empresa_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _mapa_plano_path(self, empresa_id: str) -> Path:
        return self._empresa_dir(empresa_id) / "mapa_plano.json"

    def _plano_contas_path(self, empresa_id: str) -> Path:
        return self._empresa_dir(empresa_id) / "plano_contas.xlsx"

    def _resultado_dir(self, empresa_id: str) -> Path:
        path = self._empresa_dir(empresa_id) / "resultados"
        path.mkdir(exist_ok=True)
        return path

    # =====================================================
    # PLANO DE CONTAS
    # =====================================================

    def salvar_plano_contas(self, empresa_id: str, df_plano: pd.DataFrame):
        path = self._plano_contas_path(empresa_id)
        df_plano.to_excel(path, index=False)

    def carregar_plano_contas(self, empresa_id: str) -> pd.DataFrame | None:
        path = self._plano_contas_path(empresa_id)
        if not path.exists():
            return None
        return pd.read_excel(path)

    # =====================================================
    # MAPA DE CLASSIFICAÇÃO
    # =====================================================

    def salvar_mapa_plano(self, empresa_id: str, mapa: dict):
        path = self._mapa_plano_path(empresa_id)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(mapa, f, ensure_ascii=False, indent=2)

    def carregar_mapa_plano(self, empresa_id: str) -> dict | None:
        path = self._mapa_plano_path(empresa_id)
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # =====================================================
    # RESULTADOS
    # =====================================================

    def salvar_resultado(self, empresa_id: str, periodo: str, df_resultado: pd.DataFrame) -> Path:
        path = self._resultado_dir(empresa_id) / f"conciliacao_{periodo}.parquet"
        df_resultado.to_parquet(path, index=False)
        return path

    def carregar_resultado(self, empresa_id: str, periodo: str) -> pd.DataFrame | None:
        path = self._resultado_dir(empresa_id) / f"conciliacao_{periodo}.parquet"
        if not path.exists():
            return None
        return pd.read_parquet(path)
