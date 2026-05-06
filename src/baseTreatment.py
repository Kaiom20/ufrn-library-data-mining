"""
Tratamento das bases principais — UFRN
========================================
Bases tratadas:
  - exemplares-acervo.csv  →  acervo_tratado.csv
  - exemplares.csv         →  exemplares_tratado.csv

Execute a partir da raiz do projeto:
    python src/tratamento_bases.py
"""

import pandas as pd
import numpy as np
import os
import re

# =============================================================================
# 0. CAMINHOS
# =============================================================================
ACERVO_PATH     = "data/raw/exemplares-acervo.csv"
EXEMPLARES_PATH = "data/raw/exemplares.csv"
OUTPUT_DIR      = "data/processed"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# 1. LEITURA
# =============================================================================
print("=" * 60)
print("1. LEITURA")
print("=" * 60)

acervo     = pd.read_csv(ACERVO_PATH,     sep=None, engine="python")
exemplares = pd.read_csv(EXEMPLARES_PATH, sep=None, engine="python")

print(f"  exemplares-acervo : {acervo.shape[0]:>7,} linhas x {acervo.shape[1]} colunas")
print(f"  exemplares        : {exemplares.shape[0]:>7,} linhas x {exemplares.shape[1]} colunas")

# =============================================================================
# 2. TRATAMENTO — exemplares-acervo.csv
# =============================================================================
print("\n" + "=" * 60)
print("2. TRATAMENTO — exemplares-acervo.csv")
print("=" * 60)

ac = acervo.copy()

# -------------------------------------------------------------------
# 2.1 Filtrar tipos de material físicos e circuláveis
# -------------------------------------------------------------------
TIPOS_FISICOS = [
    "Livro", "Folheto", "Dissertação", "Monografia", "Tese",
    "Relatório Acadêmico", "Projeto de Pesquisa", "Manuscrito",
    "Artigo", "Periódico"
]
antes = len(ac)
ac = ac[ac["tipo_material"].isin(TIPOS_FISICOS)].copy()
print(f"\n  [2.1] tipo_material — {antes - len(ac):,} registros de mídia não circulável removidos")
print(f"        Mantidos: {len(ac):,} registros")
print(f"        Tipos mantidos: {sorted(ac['tipo_material'].unique())}")

# -------------------------------------------------------------------
# 2.2 Padronizar strings de texto
# -------------------------------------------------------------------
COLUNAS_TEXTO = ["titulo", "sub_titulo", "autor", "editora", "assunto", "edicao"]
for col in COLUNAS_TEXTO:
    if col in ac.columns:
        ac[col] = ac[col].astype(str).str.strip()
        ac[col] = ac[col].replace("nan", np.nan)

# Remover sufixo " /" no final de títulos (artefato do padrão MARC21)
ac["titulo"] = ac["titulo"].str.rstrip(" /").str.strip()
print(f"\n  [2.2] Strings padronizadas: {COLUNAS_TEXTO}")

# -------------------------------------------------------------------
# 2.3 Tratar campo 'ano'
# -------------------------------------------------------------------
# Formatos encontrados:
#   "2008."     → ano com ponto no final (padrão MARC21)
#   "c1997."    → prefixo "c" de copyright + ponto
#   "[19--]."   → década desconhecida → NaN

def limpar_ano(valor):
    if pd.isna(valor):
        return pd.NA
    s = str(valor).strip()
    s = re.sub(r"^c", "", s)
    s = re.sub(r"[\[\].]", "", s).strip()
    if "-" in s or not s.isdigit():
        return pd.NA
    ano = int(s)
    if 1800 <= ano <= 2025:
        return ano
    return pd.NA

ac["ano"] = ac["ano"].apply(limpar_ano).astype("Int64")
nulos_ano = ac["ano"].isna().sum()
print(f"\n  [2.3] Campo 'ano' tratado")
print(f"        Nulos após limpeza: {nulos_ano:,} ({nulos_ano/len(ac):.1%})")

# -------------------------------------------------------------------
# 2.4 Tratar campo 'isbn'
# -------------------------------------------------------------------
def limpar_isbn(valor):
    if pd.isna(valor):
        return np.nan
    s = re.sub(r"[-\s]", "", str(valor)).upper()
    if re.match(r"^[\dX]{10}$|^[\dX]{13}$", s):
        return s
    return np.nan

ac["isbn"] = ac["isbn"].apply(limpar_isbn)
print(f"\n  [2.4] Campo 'isbn' tratado")
print(f"        ISBNs válidos: {ac['isbn'].notna().sum():,} ({ac['isbn'].notna().sum()/len(ac):.1%})")

# -------------------------------------------------------------------
# 2.5 Tratar campo 'assunto' — separar múltiplos assuntos
# -------------------------------------------------------------------
ac["assunto"] = ac["assunto"].astype(str).replace("nan", np.nan)
ac["assunto_lista"] = ac["assunto"].apply(
    lambda x: [s.strip().rstrip(".") for s in str(x).split("#$&") if s.strip()]
    if pd.notna(x) else np.nan
)
ac["assunto_principal"] = ac["assunto_lista"].apply(
    lambda x: x[0] if isinstance(x, list) and len(x) > 0 else np.nan
)
print(f"\n  [2.5] Campo 'assunto' separado em lista e extraído assunto_principal")

# -------------------------------------------------------------------
# 2.6 Remover coluna 'issn'
# -------------------------------------------------------------------
ac = ac.drop(columns=["issn"])
print(f"\n  [2.6] Coluna 'issn' removida (99.9% nula)")

# -------------------------------------------------------------------
# 2.7 Verificar duplicatas em registro_sistema (apenas log)
# -------------------------------------------------------------------
dup = ac.duplicated(subset=["registro_sistema"]).sum()
print(f"\n  [2.7] Duplicatas em registro_sistema: {dup} (nenhuma ação tomada)")

print(f"\n  Shape final acervo: {ac.shape}")
print(f"  Nulos restantes:\n{ac.isnull().sum().to_string()}")

# =============================================================================
# 3. TRATAMENTO — exemplares.csv
# =============================================================================
print("\n" + "=" * 60)
print("3. TRATAMENTO — exemplares.csv")
print("=" * 60)

ex = exemplares.copy()

# -------------------------------------------------------------------
# 3.1 Limpar sufixo de data no codigo_barras
# -------------------------------------------------------------------
# Padrão encontrado: "L000002_16/09/2024 12:24:24"
# Causa: erro de sistema ao gravar timestamp junto ao código
def limpar_codigo_barras(codigo):
    if pd.isna(codigo):
        return np.nan
    s = str(codigo).strip()
    s = re.sub(r"_\d{2}/\d{2}/\d{4}.*$", "", s)
    return s.strip()

ex["codigo_barras"] = ex["codigo_barras"].apply(limpar_codigo_barras)
print(f"\n  [3.1] Sufixos de data removidos de 'codigo_barras'")

# -------------------------------------------------------------------
# 3.2 Registrar duplicatas (apenas log, sem remoção)
# -------------------------------------------------------------------
dup_id = ex.duplicated(subset=["id_exemplar"]).sum()
dup_cb = ex.duplicated(subset=["codigo_barras"]).sum()
print(f"\n  [3.2] Duplicatas identificadas (mantidas para análise futura):")
print(f"        id_exemplar   duplicados: {dup_id:,}")
print(f"        codigo_barras duplicados: {dup_cb:,}")
print(f"        ATENÇÃO: considerar tratamento antes da análise de empréstimos")

# -------------------------------------------------------------------
# 3.3 Remover o exemplar com biblioteca 'BSC03'
# -------------------------------------------------------------------
antes = len(ex)
ex = ex[ex["biblioteca"] != "BSC03"].copy()
print(f"\n  [3.3] Biblioteca 'BSC03' removida ({antes - len(ex)} registro)")

# -------------------------------------------------------------------
# 3.4 Padronizar strings
# -------------------------------------------------------------------
for col in ["colecao", "biblioteca", "status_material", "localizacao"]:
    ex[col] = ex[col].astype(str).str.strip()
    ex[col] = ex[col].replace("nan", np.nan)
print(f"\n  [3.4] Strings padronizadas")

# -------------------------------------------------------------------
# 3.5 Remover exemplares com registro_sistema sem correspondência no acervo
# -------------------------------------------------------------------
ids_acervo_validos = set(ac["registro_sistema"].dropna())
antes = len(ex)
ex = ex[ex["registro_sistema"].isin(ids_acervo_validos)].copy()
print(f"\n  [3.5] Exemplares órfãos (sem título no acervo) removidos: {antes - len(ex):,}")

# -------------------------------------------------------------------
# 3.6 Criar coluna 'circulavel'
# -------------------------------------------------------------------
COLECOES_CIRCULAVEIS = {
    "Acervo Circulante", "Acervo de Desbaste",
    "Obras de Referência", "Publicações de Autores do RN",
    "Publicações da UFRN", "Coleção Mossoroense",
    "Literatura de Cordel", "Folhetos", "Monografias",
    "Dissertações", "Teses", "Trabalho Acadêmico",
}
ex["circulavel"] = (
    (ex["colecao"].isin(COLECOES_CIRCULAVEIS)) &
    (ex["status_material"] != "NÃO CIRCULA")
)
print(f"\n  [3.6] Coluna 'circulavel' criada")
print(f"        Circuláveis    : {ex['circulavel'].sum():,}")
print(f"        Não circuláveis: {(~ex['circulavel']).sum():,}")

print(f"\n  Shape final exemplares: {ex.shape}")
print(f"  Nulos restantes:\n{ex.isnull().sum().to_string()}")

# =============================================================================
# 4. EXPORTAÇÃO
# =============================================================================
print("\n" + "=" * 60)
print("4. EXPORTAÇÃO")
print("=" * 60)

ac.to_csv(os.path.join(OUTPUT_DIR, "acervo_tratado.csv"), index=False)
ex.to_csv(os.path.join(OUTPUT_DIR, "exemplares_tratado.csv"), index=False)

print(f"  acervo_tratado.csv     → {ac.shape[0]:,} linhas x {ac.shape[1]} colunas")
print(f"  exemplares_tratado.csv → {ex.shape[0]:,} linhas x {ex.shape[1]} colunas")
print(f"  Salvos em: {OUTPUT_DIR}/")

print("\n" + "=" * 60)
print("TRATAMENTO CONCLUÍDO")
print("=" * 60)