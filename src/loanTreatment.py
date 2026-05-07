"""
Tratamento e consolidação dos empréstimos — UFRN (2018–2022)
=============================================================
Lê os 10 arquivos de empréstimos (2 semestres por ano),
aplica limpeza padronizada e consolida em uma única base.

Entrada : data/raw/emprestimos-XXXXXX.csv  (10 arquivos)
Saída   : data/processed/emprestimos_consolidado.csv

Execute a partir da raiz do projeto:
    python src/tratamento_emprestimos.py
"""

import pandas as pd
import numpy as np
import os
import re

# =============================================================================
# 0. CONFIGURAÇÃO
# =============================================================================
RAW_DIR    = "data/raw"
OUTPUT_DIR = "data/processed"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "emprestimos_consolidado.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Semestres disponíveis com flag de pandemia
# Formato: (nome_arquivo, ano, semestre, periodo_pandemia)
SEMESTRES = [
    ("emprestimos-20181.csv", 2018, 1, False),
    ("emprestimos-20182.csv", 2018, 2, False),
    ("emprestimos-20191.csv", 2019, 1, False),
    ("emprestimos-20192.csv", 2019, 2, False),
    ("emprestimos-20201.csv", 2020, 1, False),  # começo da pandemia, ainda razoável
    ("emprestimos-20202.csv", 2020, 2, True),   # biblioteca fechada (101 registros)
    ("emprestimos-20211.csv", 2021, 1, True),   # biblioteca fechada (872 registros)
    ("emprestimos-20212.csv", 2021, 2, True),   # retomada parcial (4.365 registros)
    ("emprestimos-20221.csv", 2022, 1, False),  # retorno normal
    ("emprestimos-20222.csv", 2022, 2, False),
]

# =============================================================================
# 1. FUNÇÃO DE LIMPEZA POR SEMESTRE
# =============================================================================
def limpar_codigo_barras(codigo):
    """Remove sufixo de data em codigos_barras como 'L000002_16/09/2024 12:24:24'."""
    if pd.isna(codigo):
        return np.nan
    s = str(codigo).strip()
    s = re.sub(r"_\d{2}/\d{2}/\d{4}.*$", "", s)
    return s.strip()


def tratar_semestre(arquivo, ano, semestre, pandemia):
    """Lê e trata um arquivo de empréstimos de um semestre."""
    caminho = os.path.join(RAW_DIR, arquivo)
    df = pd.read_csv(caminho, sep=None, engine="python")

    # --- Colunas de data ---
    for col in ["data_emprestimo", "data_devolucao", "data_renovacao"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # --- Remover sem data_emprestimo (essencial) ---
    antes = len(df)
    df = df.dropna(subset=["data_emprestimo"])
    removidos_data = antes - len(df)

    # --- Limpar codigo_barras ---
    df["codigo_barras"] = df["codigo_barras"].apply(limpar_codigo_barras)

    # --- Remover duplicatas por id_emprestimo ---
    antes = len(df)
    df = df.drop_duplicates(subset=["id_emprestimo"])
    removidos_dup = antes - len(df)

    # --- Calcular duração do empréstimo ---
    df["duracao_dias"] = (df["data_devolucao"] - df["data_emprestimo"]).dt.days
    dur_invalida = df["duracao_dias"] < 0
    df.loc[dur_invalida, "duracao_dias"] = np.nan

    # --- Flags auxiliares ---
    df["foi_renovado"]  = df["data_renovacao"].notna()
    df["foi_devolvido"] = df["data_devolucao"].notna()

    # --- Padronizar tipo_vinculo_usuario ---
    df["tipo_vinculo_usuario"] = df["tipo_vinculo_usuario"].astype(str).str.strip().str.upper()

    # --- Remover coluna nome_usuario (dado pessoal, não necessário) ---
    if "nome_usuario" in df.columns:
        df = df.drop(columns=["nome_usuario"])

    # --- Colunas de controle ---
    df["ano"]              = ano
    df["semestre"]         = semestre
    df["periodo_pandemia"] = pandemia

    print(f"  {arquivo}: {len(df):>7,} registros "
          f"| removidos data={removidos_data} dup={removidos_dup} "
          f"{'⚠ PANDEMIA' if pandemia else ''}")

    return df


# =============================================================================
# 2. PROCESSAR E CONSOLIDAR TODOS OS SEMESTRES
# =============================================================================
print("=" * 60)
print("TRATAMENTO DE EMPRÉSTIMOS — 2018 a 2022")
print("=" * 60)
print()

semestres_processados = []

for arquivo, ano, semestre, pandemia in SEMESTRES:
    df_sem = tratar_semestre(arquivo, ano, semestre, pandemia)
    semestres_processados.append(df_sem)

# Concatenar tudo
consolidado = pd.concat(semestres_processados, ignore_index=True)

# =============================================================================
# 3. VALIDAÇÕES PÓS-CONSOLIDAÇÃO
# =============================================================================
print()
print("=" * 60)
print("VALIDAÇÕES PÓS-CONSOLIDAÇÃO")
print("=" * 60)

print(f"\n  Total de registros consolidados : {len(consolidado):,}")
print(f"  Período coberto                 : "
      f"{consolidado['data_emprestimo'].min().date()} → "
      f"{consolidado['data_emprestimo'].max().date()}")

# Duplicatas entre semestres (mesmo id_emprestimo em arquivos diferentes)
dup_cross = consolidado.duplicated(subset=["id_emprestimo"]).sum()
print(f"  Duplicatas entre semestres      : {dup_cross:,}")
if dup_cross > 0:
    consolidado = consolidado.drop_duplicates(subset=["id_emprestimo"])
    print(f"  → Removidas. Total final        : {len(consolidado):,}")

print(f"\n  Registros por ano:")
print(consolidado.groupby("ano")["id_emprestimo"].count().to_string())

print(f"\n  Registros por semestre (ano x semestre):")
print(consolidado.groupby(["ano", "semestre"])["id_emprestimo"].count().to_string())

print(f"\n  Registros período pandemia      : "
      f"{consolidado['periodo_pandemia'].sum():,} "
      f"({consolidado['periodo_pandemia'].mean():.1%})")

print(f"\n  tipo_vinculo_usuario:")
print(f"  {consolidado['tipo_vinculo_usuario'].value_counts().to_string()}")

print(f"\n  Nulos restantes:")
print(f"  {consolidado.isnull().sum().to_string()}")

# =============================================================================
# 4. EXPORTAÇÃO
# =============================================================================
print()
print("=" * 60)
print("EXPORTAÇÃO")
print("=" * 60)

consolidado.to_csv(OUTPUT_PATH, index=False)

print(f"\n  emprestimos_consolidado.csv → {len(consolidado):,} linhas x {consolidado.shape[1]} colunas")
print(f"  Salvo em: {OUTPUT_PATH}")

print("\n" + "=" * 60)
print("CONSOLIDAÇÃO CONCLUÍDA")
print("=" * 60)