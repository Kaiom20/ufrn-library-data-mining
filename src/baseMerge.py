"""
Merge das bases filtradas — Área de Sistemas de Informação
===========================================================
Une as 3 bases tratadas e filtradas em uma base unificada,
mantendo todos os exemplares (inclusive os nunca emprestados).

Entradas:
  - data/processed/exemplares_trat_filtrado.csv
  - data/processed/acervo_trat_filtrado.csv
  - data/processed/emprestimos_consolidado.csv

Saída:
  - data/processed/base_unificada.csv

Execute a partir da raiz do projeto:
    python src/merge_bases.py
"""

import pandas as pd
import numpy as np
import os

# =============================================================================
# 0. CAMINHOS
# =============================================================================
EXEMPLARES_PATH  = "data/processed/exemplares_trat_filtrado.csv"
ACERVO_PATH      = "data/processed/acervo_trat_filtrado.csv"
EMPRESTIMOS_PATH = "data/processed/emprestimos_consolidado.csv"
OUTPUT_PATH      = "data/processed/base_unificada.csv"

# =============================================================================
# 1. LEITURA
# =============================================================================
print("=" * 60)
print("MERGE DAS BASES — Área de Sistemas de Informação")
print("=" * 60)

ex  = pd.read_csv(EXEMPLARES_PATH)
ac  = pd.read_csv(ACERVO_PATH)
emp = pd.read_csv(EMPRESTIMOS_PATH)

print(f"\n  exemplares_trat_filtrado : {len(ex):>7,} linhas")
print(f"  acervo_trat_filtrado     : {len(ac):>7,} linhas")
print(f"  emprestimos_consolidado  : {len(emp):>7,} linhas")

# =============================================================================
# 2. MERGE 1 — exemplares + acervo (via registro_sistema)
# =============================================================================
# Left join: mantém todos os exemplares, traz os metadados do título
ex_ac = ex.merge(
    ac[[
        "registro_sistema", "titulo", "autor",
        "assunto_principal", "tipo_material", "ano", "editora"
    ]],
    on="registro_sistema",
    how="left"
)

sem_titulo = ex_ac["titulo"].isna().sum()
print(f"\n  [Merge 1] exemplares + acervo")
print(f"  Resultado  : {len(ex_ac):,} linhas x {ex_ac.shape[1]} colunas")
print(f"  Sem título : {sem_titulo} exemplares sem correspondência no acervo")

# =============================================================================
# 3. FILTRAR EMPRÉSTIMOS PARA A ÁREA DE SI
# =============================================================================
# Os empréstimos estão na base geral (todas as áreas).
# Filtramos apenas os empréstimos cujo codigo_barras pertence
# aos exemplares da área de SI.
cb_si = set(ex["codigo_barras"].dropna())
emp_si = emp[emp["codigo_barras"].isin(cb_si)].copy()

print(f"\n  [Filtro empréstimos] código de barras da área de SI")
print(f"  Empréstimos totais     : {len(emp):,}")
print(f"  Empréstimos da área SI : {len(emp_si):,} ({len(emp_si)/len(emp):.1%})")
print(f"  Distribuição por ano:")
print(f"  {emp_si['ano'].value_counts().sort_index().to_string()}")

# =============================================================================
# 4. MERGE 2 — exemplares+acervo + empréstimos (via codigo_barras)
# =============================================================================
# Left join: mantém todos os exemplares, inclusive os nunca emprestados
# Cada linha será um empréstimo; exemplares sem empréstimo terão NaN
base = ex_ac.merge(
    emp_si[[
        "id_emprestimo", "codigo_barras", "data_emprestimo",
        "data_devolucao", "ano", "semestre",
        "duracao_dias", "foi_renovado", "foi_devolvido",
        "tipo_vinculo_usuario"
    ]],
    on="codigo_barras",
    how="left",
    suffixes=("_acervo", "_emprestimo")
)

# Renomear colunas de ano para evitar ambiguidade
# ano_acervo = ano de publicação do livro
# ano_emprestimo = ano em que ocorreu o empréstimo
base = base.rename(columns={
    "ano_acervo"    : "ano_publicacao",
    "ano_emprestimo": "ano_emprestimo"
})

print(f"\n  [Merge 2] + empréstimos")
print(f"  Resultado: {len(base):,} linhas x {base.shape[1]} colunas")

# =============================================================================
# 5. FLAGS E COLUNAS AUXILIARES
# =============================================================================
# Flag: exemplar nunca foi emprestado no período analisado
base["nunca_emprestado"] = base["id_emprestimo"].isna()

# Verificação por exemplar único
por_exemplar = base.drop_duplicates("codigo_barras")
nunca = por_exemplar["nunca_emprestado"].sum()
total = len(por_exemplar)
print(f"\n  Exemplares únicos na base   : {total:,}")
print(f"  Nunca emprestados no período: {nunca:,} ({nunca/total:.1%})")

# =============================================================================
# 6. EXPORTAÇÃO
# =============================================================================
print("\n" + "=" * 60)
print("EXPORTAÇÃO")
print("=" * 60)

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
base.to_csv(OUTPUT_PATH, index=False)

print(f"\n  base_unificada.csv → {len(base):,} linhas x {base.shape[1]} colunas")
print(f"  Salvo em: {OUTPUT_PATH}")
print(f"\n  Colunas disponíveis:")
for col in base.columns:
    print(f"    - {col}")

print("\n" + "=" * 60)
print("MERGE CONCLUÍDO")
print("=" * 60)