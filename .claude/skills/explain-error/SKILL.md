---
name: explain-error
description: Explica um erro do Airflow a partir do stack trace
invocation: user
---

Dado um stack trace do Airflow, explique:

1. **O que causou o erro** — em linguagem simples, sem jargão desnecessário
2. **Em qual camada ocorreu** — DAG parse, scheduler, executor ou task
3. **Trecho problemático** — aponte a linha exata do código que causou o erro
4. **Como corrigir** — mostre o código corrigido
5. **Como evitar** — o que fazer para esse erro não acontecer novamente

Cole o stack trace após chamar o comando.
