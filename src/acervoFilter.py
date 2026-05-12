"""
Filtro do acervo pela área de Sistemas de Informação
=====================================================
Lê os registro_sistema presentes em exemplares_trat_filtrado.csv
e filtra o acervo_tratado.csv mantendo apenas os títulos
correspondentes à área de SI (CDD 004).

Entrada : data/processed/exemplares_trat_filtrado.csv
          data/processed/acervo_tratado.csv
Saída   : data/processed/acervo_trat_filtrado.csv

Execute a partir da raiz do projeto:
    python src/filtro_acervo_si.py
"""

import pandas as pd
import os

# =============================================================================
# 0. CAMINHOS
# =============================================================================
EXEMPLARES_FILTRADO_PATH = "data/processed/exemplares_trat_filtrado.csv"
ACERVO_PATH              = "data/processed/acervo_tratado.csv"
OUTPUT_PATH              = "data/processed/acervo_trat_filtrado.csv"

# =============================================================================
# 1. LEITURA
# =============================================================================
print("=" * 60)
print("FILTRO — Acervo por Área de Sistemas de Informação")
print("=" * 60)

exemplares_filtrado = pd.read_csv(EXEMPLARES_FILTRADO_PATH)
acervo              = pd.read_csv(ACERVO_PATH)

print(f"\n  exemplares_trat_filtrado : {len(exemplares_filtrado):>7,} linhas")
print(f"  acervo_tratado           : {len(acervo):>7,} linhas")

# =============================================================================
# 2. EXTRAIR registro_sistema DO EXEMPLARES FILTRADO
# =============================================================================
ids_si = set(exemplares_filtrado["registro_sistema"].dropna().unique())
print(f"\n  registro_sistema únicos em exemplares_filtrado: {len(ids_si):,}")

# =============================================================================
# 3. FILTRAR ACERVO
# =============================================================================
acervo_filtrado = acervo[acervo["registro_sistema"].isin(ids_si)].copy()

print(f"\n  Títulos mantidos : {len(acervo_filtrado):,}")
print(f"  Títulos removidos: {len(acervo) - len(acervo_filtrado):,}")

print(f"\n  Distribuição por tipo_material:")
print(f"  {acervo_filtrado['tipo_material'].value_counts().to_string()}")

# =============================================================================
# 4. EXPORTAÇÃO
# =============================================================================
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
acervo_filtrado.to_csv(OUTPUT_PATH, index=False)

print(f"\n  Arquivo salvo: {OUTPUT_PATH}")
print(f"  Shape final  : {acervo_filtrado.shape[0]:,} linhas x {acervo_filtrado.shape[1]} colunas")

print("\n" + "=" * 60)
print("FILTRO CONCLUÍDO")
print("=" * 60)