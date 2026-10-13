---
name: spec-from-context
description: Gera uma especificação técnica de pipeline a partir de um documento de contexto
invocation: user
---

Dado um documento de contexto (e-mail, história de sprint, ata de reunião) em `docs/contexto/`, gere uma especificação técnica em `docs/specs/`:

1. **Objetivo** — o que o pipeline faz em uma frase
2. **Inputs** — arquivos, APIs ou bancos de origem com detalhes (formato, colunas, localização)
3. **Outputs** — o que é gerado, onde e em qual formato
4. **Fluxo de execução** — diagrama ASCII das tasks em sequência
5. **Tasks** — para cada task: nome, tipo, o que faz, o que recebe e o que retorna
6. **Agendamento** — frequência, horário, cron expression
7. **Tratamento de falhas** — o que fazer em cada cenário de erro
8. **Casos de borda** — situações especiais consideradas e fora do escopo

Salve o resultado em `docs/specs/SPEC_[nome].md`.

Uso: `/spec-from-context docs/contexto/email_vendas.md`
