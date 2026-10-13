---
name: gen-data
description: Gera arquivo CSV de vendas fictícias da Vende+ para testes
invocation: user
---

Gere um arquivo CSV de vendas fictícias da Vende+ para a data informada (padrão: hoje):

1. Execute o script `scripts/gerar_dados.py` para a data informada
2. Confirme que o arquivo foi gerado em `data/input/vendas_YYYY-MM-DD.csv`
3. Mostre as primeiras 5 linhas do arquivo gerado

O CSV deve ter as colunas: `data`, `pedido_id`, `produto`, `categoria`, `quantidade`, `valor_unitario`

Uso:
- `/gen-data` — gera para hoje
- `/gen-data 2025-01-15` — gera para a data informada
