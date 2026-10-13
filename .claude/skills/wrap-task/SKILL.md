---
name: wrap-task
description: Encapsula uma função Python existente como @task da TaskFlow API
invocation: user
---

Dado o nome de uma função em `helpers/`, encapsule-a como uma task usando `@task` da TaskFlow API:

1. Leia a função em `helpers/` para entender o que ela recebe e retorna
2. Crie a task na DAG chamando a função do helper — não mova a lógica para dentro da DAG
3. Adicione tipagem nos parâmetros e no retorno da task
4. Garanta que o retorno é compatível com XCom — apenas metadados pequenos (caminho de arquivo, contagem, flag)
5. Se a função retornar um DataFrame ou estrutura grande, a task deve salvar em arquivo e retornar apenas o caminho

Mostre o código da task gerada e explique o que cada parte faz.
