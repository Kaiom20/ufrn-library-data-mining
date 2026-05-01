"""
Pré-processamento dos dados de acervo e empréstimos - UFRN
=============================================================
Bases utilizadas:
  - exemplares-acervo.csv  : metadados dos títulos (registro_sistema como PK)
  - exemplares.csv         : exemplares físicos (registro_sistema como FK)
  - emprestimos-XXXXXX.csv : histórico de empréstimos (codigo_barras como FK)

Saídas geradas:
  - acervo_limpo.csv       : exemplares-acervo tratado
  - exemplares_limpo.csv   : exemplares tratado
  - emprestimos_limpo.csv  : empréstimos tratado (qualquer semestre)
  - base_unificada.csv     : merge completo das 3 bases
"""

import pandas as pd
import numpy as np
import os
import re

# =============================================================================
# 0. CONFIGURAÇÃO DE CAMINHOS
# =============================================================================
# Ajuste os caminhos abaixo conforme a localização dos seus arquivos
ACERVO_PATH      = "exemplares-acervo.csv"
EXEMPLARES_PATH  = "exemplares.csv"
EMPRESTIMOS_PATH = "emprestimos-20181.csv"   # troque pelo semestre desejado
OUTPUT_DIR       = "output"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# 1. LEITURA DAS BASES
# =============================================================================
print("=" * 60)
print("1. LEITURA DAS BASES")
print("=" * 60)

acervo      = pd.read_csv(ACERVO_PATH,      sep=None, engine="python")
exemplares  = pd.read_csv(EXEMPLARES_PATH,  sep=None, engine="python")
emprestimos = pd.read_csv(EMPRESTIMOS_PATH, sep=None, engine="python")

print(f"  exemplares-acervo : {acervo.shape[0]:>7,} linhas  | {acervo.shape[1]} colunas")
print(f"  exemplares        : {exemplares.shape[0]:>7,} linhas  | {exemplares.shape[1]} colunas")
print(f"  emprestimos       : {emprestimos.shape[0]:>7,} linhas  | {emprestimos.shape[1]} colunas")

# =============================================================================
# 2. LIMPEZA: exemplares-acervo.csv
# =============================================================================
print("\n" + "=" * 60)
print("2. LIMPEZA — exemplares-acervo.csv")
print("=" * 60)

ac = acervo.copy()

# --- 2.1 Remover tipos de material que não são livros físicos circuláveis ---
# Mantemos apenas os tipos relevantes para análise de realocação de acervo físico
TIPOS_RELEVANTES = [
    "Livro", "Folheto", "Monografia", "Dissertação", "Tese",
    "Relatório Acadêmico", "Projeto de Pesquisa"
]
antes = len(ac)
ac = ac[ac["tipo_material"].isin(TIPOS_RELEVANTES)].copy()
print(f"  [tipo_material] Removidos {antes - len(ac):,} registros fora do escopo "
      f"({len(ac):,} mantidos)")

# --- 2.2 Padronizar strings de texto ---
for col in ["titulo", "sub_titulo", "autor", "editora", "assunto"]:
    if col in ac.columns:
        ac[col] = ac[col].astype(str).str.strip()
        ac[col] = ac[col].replace("nan", np.nan)

# Remover sufixo " /" no final de títulos (padrão MARC21)
ac["titulo"] = ac["titulo"].str.rstrip(" /").str.strip()

# --- 2.3 Tratar campo 'ano' ---
# Converter para inteiro, descartando anos impossíveis
ac["ano"] = pd.to_numeric(ac["ano"], errors="coerce").astype("Int64")
ANO_MIN, ANO_MAX = 1800, 2025
invalidos_ano = ac["ano"].notna() & ~ac["ano"].between(ANO_MIN, ANO_MAX)
print(f"  [ano] {invalidos_ano.sum()} valores fora do intervalo [{ANO_MIN}–{ANO_MAX}] → NaN")
ac.loc[invalidos_ano, "ano"] = pd.NA

# --- 2.4 Tratar campo 'quantidade' ---
ac["quantidade"] = pd.to_numeric(ac["quantidade"], errors="coerce").astype("Int64")
qtd_invalida = ac["quantidade"].isna() | (ac["quantidade"] < 0)
print(f"  [quantidade] {qtd_invalida.sum()} valores inválidos → NaN")

# --- 2.5 Separar múltiplos assuntos (delimitados por #$&) ---
# Mantemos a coluna original e criamos uma versão lista
ac["assunto_lista"] = ac["assunto"].str.split(r"#\$&").apply(
    lambda x: [s.strip() for s in x] if isinstance(x, list) else np.nan
)
# Primeiro assunto como assunto principal
ac["assunto_principal"] = ac["assunto_lista"].apply(
    lambda x: x[0] if isinstance(x, list) and len(x) > 0 else np.nan
)

# --- 2.6 Remover duplicatas (nenhuma encontrada, mas garantia) ---
antes = len(ac)
ac = ac.drop_duplicates(subset=["registro_sistema"])
print(f"  [duplicatas] {antes - len(ac)} removidas")

print(f"\n  Nulos restantes por coluna:\n{ac.isnull().sum().to_string()}")
print(f"\n  Shape final: {ac.shape}")

# =============================================================================
# 3. LIMPEZA: exemplares.csv
# =============================================================================
print("\n" + "=" * 60)
print("3. LIMPEZA — exemplares.csv")
print("=" * 60)

ex = exemplares.copy()

# --- 3.1 Limpar codigo_barras com sufixo de data ---
# Padrão encontrado: "L000002_16/09/2024 12:24:24"
def limpar_codigo_barras(codigo):
    if pd.isna(codigo):
        return np.nan
    codigo = str(codigo).strip()
    # Remove sufixo "_DD/MM/AAAA HH:MM:SS"
    codigo = re.sub(r"_\d{2}/\d{2}/\d{4}.*$", "", codigo)
    return codigo.strip()

ex["codigo_barras"] = ex["codigo_barras"].apply(limpar_codigo_barras)
print(f"  [codigo_barras] Sufixos de data removidos")

# --- 3.2 Padronizar strings ---
for col in ["colecao", "biblioteca", "status_material", "localizacao"]:
    if col in ex.columns:
        ex[col] = ex[col].astype(str).str.strip()
        ex[col] = ex[col].replace("nan", np.nan)

# --- 3.3 Remover os 3 registro_sistema órfãos (sem correspondência no acervo) ---
ids_acervo = set(ac["registro_sistema"].dropna())
antes = len(ex)
orfaos_mask = ~ex["registro_sistema"].isin(ids_acervo)
print(f"  [integridade] {orfaos_mask.sum()} exemplares com registro_sistema "
      f"sem correspondência no acervo → removidos")
ex = ex[~orfaos_mask].copy()
print(f"  Removidos: {antes - len(ex)} | Mantidos: {len(ex):,}")

# --- 3.4 Filtrar apenas tipos de coleção relevantes para circulação ---
# Coleções que fazem sentido para análise de realocação física
COLECOES_RELEVANTES = [
    "Acervo Circulante",
    "Acervo de Desbaste",   # candidatos à saída — útil para análise
    "Obras de Referência",
    "Publicações de Autores do RN",
    "Publicações da UFRN",
    "Coleção Mossoroense",
]
antes = len(ex)
ex_relevante = ex[ex["colecao"].isin(COLECOES_RELEVANTES)].copy()
print(f"  [colecao] {antes - ex_relevante.shape[0]:,} exemplares fora das coleções "
      f"de interesse ({ex_relevante.shape[0]:,} mantidos)")
# Mantemos o df completo como ex e o filtrado como ex_relevante
# (o código de merge usará ex_relevante, mas você pode trocar por ex se quiser)

# --- 3.5 Verificar duplicatas em codigo_barras ---
dup_cb = ex["codigo_barras"].duplicated().sum()
print(f"  [codigo_barras] Duplicatas após limpeza: {dup_cb}")

print(f"\n  Nulos restantes por coluna:\n{ex.isnull().sum().to_string()}")
print(f"\n  Shape final (completo): {ex.shape}")
print(f"  Shape final (coleções relevantes): {ex_relevante.shape}")

# =============================================================================
# 4. LIMPEZA: emprestimos.csv
# =============================================================================
print("\n" + "=" * 60)
print("4. LIMPEZA — emprestimos.csv")
print("=" * 60)

emp = emprestimos.copy()

# --- 4.1 Converter colunas de data ---
COLUNAS_DATA = ["data_emprestimo", "data_devolucao", "data_renovacao"]
for col in COLUNAS_DATA:
    emp[col] = pd.to_datetime(emp[col], errors="coerce")
print(f"  [datas] Convertidas: {COLUNAS_DATA}")

# --- 4.2 Remover empréstimos sem data_emprestimo (essencial) ---
antes = len(emp)
emp = emp.dropna(subset=["data_emprestimo"])
print(f"  [data_emprestimo] {antes - len(emp)} linhas sem data removidas")

# --- 4.3 Calcular duração do empréstimo em dias ---
emp["duracao_dias"] = (
    emp["data_devolucao"] - emp["data_emprestimo"]
).dt.days
# Durações negativas = erro de registro
dur_invalida = emp["duracao_dias"] < 0
print(f"  [duracao_dias] {dur_invalida.sum()} durações negativas → NaN")
emp.loc[dur_invalida, "duracao_dias"] = np.nan

# --- 4.4 Flag: empréstimo foi renovado ---
emp["foi_renovado"] = emp["data_renovacao"].notna()

# --- 4.5 Flag: livro devolvido ---
emp["foi_devolvido"] = emp["data_devolucao"].notna()

# --- 4.6 Padronizar tipo_vinculo_usuario ---
emp["tipo_vinculo_usuario"] = emp["tipo_vinculo_usuario"].str.strip().str.upper()

# --- 4.7 Remover duplicatas exatas ---
antes = len(emp)
emp = emp.drop_duplicates(subset=["id_emprestimo"])
print(f"  [duplicatas] {antes - len(emp)} removidas por id_emprestimo")

print(f"\n  Nulos restantes por coluna:\n{emp.isnull().sum().to_string()}")
print(f"\n  Shape final: {emp.shape}")

# =============================================================================
# 5. MERGE: construção da base unificada
# =============================================================================
print("\n" + "=" * 60)
print("5. MERGE — construção da base unificada")
print("=" * 60)

# Passo 1: exemplares + acervo (via registro_sistema)
ex_ac = ex_relevante.merge(
    ac[[
        "registro_sistema", "titulo", "autor", "assunto_principal",
        "assunto_lista", "tipo_material", "ano", "editora", "quantidade"
    ]],
    on="registro_sistema",
    how="left"
)
print(f"  [exemplares + acervo] {ex_ac.shape[0]:,} linhas x {ex_ac.shape[1]} colunas")

# Passo 2: adicionar empréstimos (via codigo_barras)
# Usamos left join para manter todos os exemplares, inclusive os não emprestados
base = ex_ac.merge(
    emp[[
        "id_emprestimo", "codigo_barras", "data_emprestimo",
        "data_devolucao", "data_renovacao", "duracao_dias",
        "foi_renovado", "foi_devolvido", "tipo_vinculo_usuario",
        "matricula_ou_siape"
    ]],
    on="codigo_barras",
    how="left"
)
print(f"  [+ emprestimos]       {base.shape[0]:,} linhas x {base.shape[1]} colunas")

# --- 5.1 Flag: exemplar nunca emprestado neste período ---
base["nunca_emprestado"] = base["id_emprestimo"].isna()
nao_emp = base.drop_duplicates("codigo_barras")["nunca_emprestado"].sum()
total_ex = base["codigo_barras"].nunique()
print(f"\n  Exemplares únicos na base  : {total_ex:,}")
print(f"  Nunca emprestados (período): {nao_emp:,} ({nao_emp/total_ex:.1%})")

print(f"\n  Shape final base unificada: {base.shape}")

# =============================================================================
# 6. EXPORTAÇÃO
# =============================================================================
print("\n" + "=" * 60)
print("6. EXPORTAÇÃO")
print("=" * 60)

ac.to_csv(os.path.join(OUTPUT_DIR, "acervo_limpo.csv"),       index=False)
ex.to_csv(os.path.join(OUTPUT_DIR, "exemplares_limpo.csv"),   index=False)
emp.to_csv(os.path.join(OUTPUT_DIR, "emprestimos_limpo.csv"), index=False)
base.to_csv(os.path.join(OUTPUT_DIR, "base_unificada.csv"),   index=False)

print(f"  acervo_limpo.csv      → {ac.shape}")
print(f"  exemplares_limpo.csv  → {ex.shape}")
print(f"  emprestimos_limpo.csv → {emp.shape}")
print(f"  base_unificada.csv    → {base.shape}")
print(f"\n  Todos os arquivos salvos em: ./{OUTPUT_DIR}/")

print("\n" + "=" * 60)
print("PRÉ-PROCESSAMENTO CONCLUÍDO")
print("=" * 60)