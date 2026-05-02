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
# Removemos tipos que não são relevantes para realocação de acervo físico:
# Disco, Partitura, CD de Áudio, CD-ROM, DVD, Vídeo, Fotografia, etc.
TIPOS_FISICOS = [
    "Livro", "Folheto", "Dissertação", "Monografia", "Tese",
    "Relatório Acadêmico", "Projeto de Pesquisa", "Manuscrito",
    "Artigo", "Periódico"
]
antes = len(ac)
ac = ac[ac["tipo_material"].isin(TIPOS_FISICOS)].copy()
removidos = antes - len(ac)
print(f"\n  [2.1] tipo_material — {removidos:,} registros de mídia não circulável removidos")
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
#   "[19--]."   → século conhecido, década desconhecida → NaN
#   NaN         → ausente

def limpar_ano(valor):
    if pd.isna(valor):
        return pd.NA
    s = str(valor).strip()
    # Remove prefixo "c" de copyright
    s = re.sub(r"^c", "", s)
    # Remove ponto final e colchetes
    s = re.sub(r"[\[\].]", "", s).strip()
    # Se ainda tiver traços (ex: "19--"), é impreciso → NaN
    if "-" in s or not s.isdigit():
        return pd.NA
    ano = int(s)
    # Valida intervalo razoável para publicações acadêmicas
    if 1800 <= ano <= 2025:
        return ano
    return pd.NA

ac["ano"] = ac["ano"].apply(limpar_ano).astype("Int64")

nulos_ano = ac["ano"].isna().sum()
print(f"\n  [2.3] Campo 'ano' tratado")
print(f"        Valores nulos após limpeza: {nulos_ano:,} ({nulos_ano/len(ac):.1%})")
print(f"        Distribuição por década:\n"
      f"{ac['ano'].dropna().apply(lambda x: f'{(x//10)*10}s').value_counts().sort_index().to_string()}")

# -------------------------------------------------------------------
# 2.4 Tratar campo 'isbn'
# -------------------------------------------------------------------
# Remover registros claramente inválidos como "(broch.)."
def limpar_isbn(valor):
    if pd.isna(valor):
        return np.nan
    s = str(valor).strip()
    # ISBN válido: apenas dígitos, hífens e 'X' no final
    s_clean = re.sub(r"[-\s]", "", s).upper()
    if re.match(r"^[\dX]{10}$|^[\dX]{13}$", s_clean):
        return s_clean
    return np.nan

ac["isbn"] = ac["isbn"].apply(limpar_isbn)
validos_isbn = ac["isbn"].notna().sum()
print(f"\n  [2.4] Campo 'isbn' tratado")
print(f"        ISBNs válidos: {validos_isbn:,} ({validos_isbn/len(ac):.1%})")

# -------------------------------------------------------------------
# 2.5 Tratar campo 'assunto' — separar múltiplos assuntos
# -------------------------------------------------------------------
# O campo usa "#$&" como separador entre assuntos (padrão MARC21)
ac["assunto"] = ac["assunto"].astype(str).replace("nan", np.nan)

ac["assunto_lista"] = ac["assunto"].apply(
    lambda x: [s.strip().rstrip(".") for s in str(x).split("#$&") if s.strip()]
    if pd.notna(x) else np.nan
)
ac["assunto_principal"] = ac["assunto_lista"].apply(
    lambda x: x[0] if isinstance(x, list) and len(x) > 0 else np.nan
)
print(f"\n  [2.5] Campo 'assunto' separado em lista")
print(f"        Registros com assunto: {ac['assunto_principal'].notna().sum():,}")
print(f"        Registros sem assunto: {ac['assunto_principal'].isna().sum():,}")

# -------------------------------------------------------------------
# 2.6 Remover coluna 'issn'
# -------------------------------------------------------------------
# issn tem 99.9% de valores nulos (só faz sentido para periódicos, que foram
# removidos em 2.1). Mantemos isbn que tem cobertura razoável.
ac = ac.drop(columns=["issn"])
print(f"\n  [2.6] Coluna 'issn' removida (99.9% nula, exclusiva de periódicos)")

# -------------------------------------------------------------------
# 2.7 Verificar duplicatas em registro_sistema
# -------------------------------------------------------------------
dup = ac.duplicated(subset=["registro_sistema"]).sum()
print(f"\n  [2.7] Duplicatas em registro_sistema: {dup}")
if dup > 0:
    ac = ac.drop_duplicates(subset=["registro_sistema"])
    print(f"        {dup} duplicatas removidas")

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
# 3.2 Tratar duplicatas em id_exemplar
# -------------------------------------------------------------------
# Encontradas 280 linhas duplicadas em id_exemplar.
# Causa: o mesmo id_exemplar aparece com codigo_barras sujo (com data)
# e limpo. Após limpeza em 3.1, ficaram registros idênticos.
# Estratégia: manter a versão com codigo_barras mais curto (o limpo).
antes = len(ex)
ex = ex.sort_values("codigo_barras").drop_duplicates(
    subset=["id_exemplar"], keep="first"
)
print(f"\n  [3.2] Duplicatas em id_exemplar")
print(f"        Removidas: {antes - len(ex)} | Mantidos: {len(ex):,}")

# -------------------------------------------------------------------
# 3.3 Tratar duplicatas em codigo_barras
# -------------------------------------------------------------------
# 2.713 linhas com codigo_barras duplicado entre diferentes id_exemplar.
# Causa: erro de cadastro — mesmo código de barras atribuído a 2 exemplares.
# Estratégia: manter o registro com menor id_exemplar (mais antigo/original).
antes = len(ex)
ex = ex.sort_values("id_exemplar").drop_duplicates(
    subset=["codigo_barras"], keep="first"
)
print(f"\n  [3.3] Duplicatas em codigo_barras")
print(f"        Removidas: {antes - len(ex)} | Mantidos: {len(ex):,}")

# -------------------------------------------------------------------
# 3.4 Remover o exemplar com biblioteca 'BSC03'
# -------------------------------------------------------------------
# BSC03 é um código interno sem correspondência a uma biblioteca real.
# Apenas 1 registro afetado.
antes = len(ex)
ex = ex[ex["biblioteca"] != "BSC03"].copy()
print(f"\n  [3.4] Biblioteca 'BSC03' removida ({antes - len(ex)} registro)")

# -------------------------------------------------------------------
# 3.5 Padronizar strings
# -------------------------------------------------------------------
for col in ["colecao", "biblioteca", "status_material", "localizacao"]:
    ex[col] = ex[col].astype(str).str.strip()
    ex[col] = ex[col].replace("nan", np.nan)

print(f"\n  [3.5] Strings padronizadas")

# -------------------------------------------------------------------
# 3.6 Remover exemplares com registro_sistema sem correspondência no acervo
# -------------------------------------------------------------------
ids_acervo_validos = set(ac["registro_sistema"].dropna())
antes = len(ex)
ex = ex[ex["registro_sistema"].isin(ids_acervo_validos)].copy()
print(f"\n  [3.6] Exemplares órfãos (sem título no acervo) removidos: {antes - len(ex)}")

# -------------------------------------------------------------------
# 3.7 Criar coluna 'circulavel'
# -------------------------------------------------------------------
# Booleano que indica se o exemplar pode ser emprestado.
# Útil para a análise de realocação: só faz sentido realocar exemplares
# que estão aptos à circulação.
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
nao_circ = (~ex["circulavel"]).sum()
print(f"\n  [3.7] Coluna 'circulavel' criada")
print(f"        Circuláveis    : {ex['circulavel'].sum():,}")
print(f"        Não circuláveis: {nao_circ:,}")

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