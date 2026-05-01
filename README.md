# Library Book Reallocation – UFRN

Projeto de mineração de dados sobre o acervo e os empréstimos das bibliotecas
da Universidade Federal do Rio Grande do Norte (UFRN), desenvolvido como
Trabalho de Conclusão de Curso.

## Objetivo

Propor uma estratégia de realocação e reaproveitamento de exemplares subutilizados
entre as bibliotecas da UFRN, evitando descarte desnecessário e otimizando a
disponibilidade do acervo para os usuários.

## Bases de dados

| Arquivo                  | Descrição                                      | Linhas     |
|--------------------------|------------------------------------------------|------------|
| `exemplares-acervo.csv`  | Metadados dos títulos do acervo                | ~259 mil   |
| `exemplares.csv`         | Exemplares físicos e sua localização           | ~621 mil   |
| `emprestimos-XXXX.csv`   | Histórico de empréstimos (2018–2023)           | ~100 mil/semestre |

## Estrutura do projeto
```
├── data/
│   ├── raw/          # Dados originais (não versionados)
│   └── processed/    # Dados tratados
├── notebooks/        # Análises exploratórias
├── src/              # Scripts de pré-processamento
└── reports/          # Gráficos e relatórios
```

## Tecnologias

- Python 3.x
- pandas · numpy

## Autor

Kaio Márcio Araújo Cavalcante Lira — Sistemas de Informação, UFRN — 2026
