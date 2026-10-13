---
name: dag-from-spec
description: Gera uma DAG completa a partir de uma especificação técnica validada
invocation: user
---

Dado um arquivo de spec em `docs/specs/`, gere a DAG correspondente em `dags/`:

1. Leia a spec completamente antes de gerar qualquer código
2. Verifique se os helpers necessários existem em `helpers/` — use-os, não reimplemente a lógica
3. Gere a DAG seguindo todos os padrões do `CLAUDE.md`:
   - `@task` e `@dag` da TaskFlow API
   - `start_date` com data fixa
   - `catchup=False`
   - `default_args` com `owner` e `retries`
   - XCom apenas para metadados
   - Sem configuração hardcoded
4. Após gerar, execute `/review-dag` automaticamente no arquivo gerado
5. Corrija qualquer problema encontrado pelo review antes de entregar

Uso: `/dag-from-spec docs/specs/SPEC_vendas.md`
