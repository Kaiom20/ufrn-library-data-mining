"""
Filtro de exemplares — Área de Sistemas de Informação (CDD 004)
================================================================
Filtra o exemplares_tratado.csv mantendo apenas exemplares
classificados na área de Computação/Sistemas de Informação,
identificados pelo código CDD 004 no campo 'localizacao'.

Dois grupos são incluídos:
  - Primários : localizacao começa com '004' (ex: 004.43, 004.7)
  - Correlatos: localizacao contém ':004' (ex: 658:004, 34:004)

Entrada : data/processed/exemplares_tratado.csv
Saída   : data/processed/exemplares_trat_filtrado.csv

Execute a partir da raiz do projeto:
    python src/filtro_area_si.py
"""

import pandas as pd
import re
import os

# =============================================================================
# 0. CAMINHOS
# =============================================================================
INPUT_PATH = "data/processed/exemplares_tratado.csv"
OUTPUT_PATH = "data/processed/exemplares_trat_filtrado.csv"

# =============================================================================
# 1. LEITURA
# =============================================================================
print("=" * 60)
print("FILTRO — Área de Sistemas de Informação (CDD 004)")
print("=" * 60)

ex = pd.read_csv(INPUT_PATH)
print(f"\n  Exemplares carregados: {len(ex):,}")

# =============================================================================
# 2. FILTRO
# =============================================================================

# Normaliza a coluna para comparação (strip de espaços extras)
loc = ex["localizacao"].astype(str).str.strip()

# --- Grupo 1: classificação primária em 004 ---
# Padrão: começa com "004" seguido de espaço, ponto ou fim de string
# Exemplos que ENTRAM : "004 M296e", "004.43 A811f", "004.738.5 N163e"
# Exemplos que NÃO entram: "37:004 T135i" (correlato), "004.4INTEL" (raro)
mask_primario = loc.str.match(r"^004[\s.]", na=False)

# --- Grupo 2: áreas correlatas com :004 ---
# Padrão: contém ":004" em qualquer posição
# Exemplos: "658:004 W422g", "34:004 D598", "519.22:004 M593"
mask_correlato = loc.str.contains(r":004", na=False)

# Máscara combinada
mask_total = mask_primario | mask_correlato

# =============================================================================
# 3. APLICAR E GERAR COLUNA DE CLASSIFICAÇÃO
# =============================================================================
ex_filtrado = ex[mask_total].copy()

# Coluna auxiliar para identificar o grupo de cada exemplar
ex_filtrado["grupo_cdd"] = "correlato_004"
ex_filtrado.loc[
    ex_filtrado["localizacao"].astype(str).str.strip().str.match(r"^004[\s.]", na=False),
    "grupo_cdd"
] = "primario_004"

# =============================================================================
# 4. RELATÓRIO
# =============================================================================
print(f"\n  Exemplares com CDD 004 (primário) : {mask_primario.sum():>7,}")
print(f"  Exemplares com ':004' (correlatos): {mask_correlato.sum():>7,}")
print(f"  Total após filtro                  : {len(ex_filtrado):>7,}")
print(f"  Redução                            : {len(ex) - len(ex_filtrado):>7,} exemplares removidos")

print(f"\n  Distribuição por grupo_cdd:")
print(f"  {ex_filtrado['grupo_cdd'].value_counts().to_string()}")

print(f"\n  Distribuição por biblioteca:")
print(f"  {ex_filtrado['biblioteca'].value_counts().to_string()}")

print(f"\n  Distribuição por coleção:")
print(f"  {ex_filtrado['colecao'].value_counts().to_string()}")

# =============================================================================
# 5. EXPORTAÇÃO
# =============================================================================
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
ex_filtrado.to_csv(OUTPUT_PATH, index=False)

print(f"\n  Arquivo salvo: {OUTPUT_PATH}")
print(f"  Shape final  : {ex_filtrado.shape[0]:,} linhas x {ex_filtrado.shape[1]} colunas")

print("\n" + "=" * 60)
print("FILTRO CONCLUÍDO")
print("=" * 60)