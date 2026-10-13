---
name: cron
description: Gera ou explica cron expressions para agendamento de DAGs
invocation: user
---

Dado uma descrição em português do agendamento desejado, retorne:

1. **Cron expression** — no formato `minuto hora dia mês dia-da-semana`
2. **Preset do Airflow** — se existir equivalente (`@daily`, `@hourly`, `@weekly`, `@monthly`)
3. **Explicação** — o que a expressão significa em português
4. **Próximas 3 execuções** — com data e hora exatas

Se receber uma cron expression pronta, explique o que ela faz.

Exemplos de uso:
- "todo dia às 6h da manhã"
- "toda segunda-feira às 8h"
- "a cada hora nos dias úteis"
- "0 5 * * 1-5"
