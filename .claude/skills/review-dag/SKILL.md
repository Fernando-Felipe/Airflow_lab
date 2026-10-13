---
name: review-dag
description: Revisa uma DAG e verifica se está seguindo os padrões do projeto
invocation: user
---

Revise o arquivo de DAG informado e verifique os seguintes pontos:

1. **TaskFlow API** — está usando `@task` e `@dag`? Se usar PythonOperator tradicional, aponte e sugira a versão com `@task`
2. **start_date** — é uma data fixa? Se usar `datetime.now()` ou `days_ago()`, aponte o problema
3. **catchup** — está explicitamente definido como `False`?
4. **Lógica de negócio** — há lógica de negócio dentro da DAG em vez de em `helpers/`? Aponte o que deve ser movido
5. **XCom** — há uso de XCom para passar DataFrames, listas grandes ou conteúdo de arquivo? Aponte e sugira a correção
6. **Configuração hardcoded** — há URLs, caminhos ou credenciais hardcoded? Aponte o que deve virar Variable ou Connection
7. **Nomenclatura** — dag_id e task_id estão em snake_case com verbo?
8. **default_args** — tem pelo menos `owner` e `retries` definidos?

Para cada problema encontrado, mostre o trecho problemático e a versão corrigida.
