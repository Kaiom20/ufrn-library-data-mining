"""
Tabela de Utilização do Acervo — Área de Sistemas de Informação
================================================================
Gera a tabela de utilização por título e biblioteca, conforme
o modelo do quadro branco:

  registro_sistema | titulo | biblioteca | exemplares | 2018 | 2019 | 2020 | 2022 | total | media_por_exemplar

Onde:
  - exemplares          = total de exemplares do título naquela biblioteca
  - 2018..2022          = total de empréstimos por ano
  - total               = soma dos empréstimos no período
  - media_por_exemplar  = total / exemplares (taxa de utilização)

Entrada : data/processed/base_unificada.csv
Saída   : data/processed/tabela_utilizacao.csv

Execute a partir da raiz do projeto:
    python src/tabela_utilizacao.py
"""

import pandas as pd
import numpy as np
import os

# =============================================================================
# 0. CAMINHOS
# =============================================================================
INPUT_PATH  = "data/processed/base_unificada.csv"
OUTPUT_PATH = "data/processed/tabela_utilizacao.csv"

ANOS = [2018, 2019, 2020, 2022]

# =============================================================================
# 1. LEITURA
# =============================================================================
print("=" * 60)
print("TABELA DE UTILIZAÇÃO — Área de Sistemas de Informação")
print("=" * 60)

base = pd.read_csv(INPUT_PATH)
print(f"\n  Base unificada carregada: {len(base):,} linhas")

# =============================================================================
# 2. CONTAGEM DE EXEMPLARES POR TÍTULO E BIBLIOTECA
# =============================================================================
# Um exemplar é único por codigo_barras.
# Agrupamos por (registro_sistema, biblioteca) e contamos exemplares distintos.
exemplares_por_titulo_bib = (
    base.drop_duplicates(subset=["codigo_barras"])
    .groupby(["registro_sistema", "biblioteca"])
    .agg(
        titulo           = ("titulo", "first"),
        autor            = ("autor", "first"),
        assunto_principal= ("assunto_principal", "first"),
        tipo_material    = ("tipo_material", "first"),
        exemplares       = ("codigo_barras", "count")
    )
    .reset_index()
)

print(f"\n  Combinações título x biblioteca: {len(exemplares_por_titulo_bib):,}")

# =============================================================================
# 3. CONTAGEM DE EMPRÉSTIMOS POR ANO
# =============================================================================
# Filtramos apenas linhas com empréstimo real (id_emprestimo não nulo)
emp_validos = base.dropna(subset=["id_emprestimo"]).copy()
emp_validos["ano_emprestimo"] = emp_validos["ano_emprestimo"].astype(int)

# Pivot: linhas = (registro_sistema, biblioteca), colunas = anos
emp_pivot = (
    emp_validos
    .groupby(["registro_sistema", "biblioteca", "ano_emprestimo"])
    ["id_emprestimo"]
    .count()
    .unstack(fill_value=0)
    .reset_index()
)

# Garantir que todos os anos existam como colunas (mesmo que zerados)
for ano in ANOS:
    if ano not in emp_pivot.columns:
        emp_pivot[ano] = 0

# Renomear colunas de ano para formato legível
emp_pivot = emp_pivot.rename(
    columns={ano: f"emp_{ano}" for ano in ANOS}
)

print(f"  Empréstimos por ano calculados")

# =============================================================================
# 4. JUNTAR EXEMPLARES + EMPRÉSTIMOS
# =============================================================================
tabela = exemplares_por_titulo_bib.merge(
    emp_pivot[["registro_sistema", "biblioteca"] + [f"emp_{a}" for a in ANOS]],
    on=["registro_sistema", "biblioteca"],
    how="left"
)

# Preencher NaN com 0 (títulos sem nenhum empréstimo no período)
for ano in ANOS:
    tabela[f"emp_{ano}"] = tabela[f"emp_{ano}"].fillna(0).astype(int)

# =============================================================================
# 5. CALCULAR TOTAIS E MÉDIA
# =============================================================================
cols_emp = [f"emp_{a}" for a in ANOS]

tabela["total_emprestimos"] = tabela[cols_emp].sum(axis=1)

# Média por exemplar = total de empréstimos / número de exemplares
# Representa quantas vezes cada exemplar foi emprestado, em média, no período
tabela["media_por_exemplar"] = (
    tabela["total_emprestimos"] / tabela["exemplares"]
).round(2)

# =============================================================================
# 6. ORDENAR E FORMATAR
# =============================================================================
# Ordena por: biblioteca, depois por media_por_exemplar (crescente)
# Os candidatos à realocação ficam no topo (menor uso)
tabela = tabela.sort_values(
    ["biblioteca", "media_por_exemplar"],
    ascending=[True, True]
).reset_index(drop=True)

# Reordenar colunas na ordem do quadro branco
colunas_finais = [
    "registro_sistema", "titulo", "autor", "assunto_principal",
    "tipo_material", "biblioteca", "exemplares"
] + cols_emp + ["total_emprestimos", "media_por_exemplar"]

tabela = tabela[colunas_finais]

# =============================================================================
# 7. RELATÓRIO
# =============================================================================
print(f"\n  Shape da tabela final: {tabela.shape}")

print(f"\n  Exemplares por biblioteca (top 10):")
bib_summary = (
    tabela.groupby("biblioteca")
    .agg(
        titulos    =("registro_sistema", "count"),
        exemplares =("exemplares", "sum"),
        emp_total  =("total_emprestimos", "sum")
    )
    .sort_values("exemplares", ascending=False)
    .head(10)
)
print(bib_summary.to_string())

print(f"\n  Top 10 títulos com MENOR utilização (candidatos à realocação):")
print(
    tabela[tabela["exemplares"] >= 2]
    .nsmallest(10, "media_por_exemplar")
    [["titulo", "biblioteca", "exemplares", "total_emprestimos", "media_por_exemplar"]]
    .to_string(index=False)
)

print(f"\n  Top 10 títulos com MAIOR utilização:")
print(
    tabela.nlargest(10, "media_por_exemplar")
    [["titulo", "biblioteca", "exemplares", "total_emprestimos", "media_por_exemplar"]]
    .to_string(index=False)
)

# =============================================================================
# 8. EXPORTAÇÃO
# =============================================================================
print("\n" + "=" * 60)
print("EXPORTAÇÃO")
print("=" * 60)

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
tabela.to_csv(OUTPUT_PATH, index=False)

print(f"\n  tabela_utilizacao.csv → {tabela.shape[0]:,} linhas x {tabela.shape[1]} colunas")
print(f"  Salvo em: {OUTPUT_PATH}")

print("\n" + "=" * 60)
print("TABELA GERADA COM SUCESSO")
print("=" * 60)