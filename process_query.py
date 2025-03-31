import os
import re
import http.client
import json

# Captura variáveis do ambiente definidas pelo GitHub Actions
issue_number = os.getenv("ISSUE_NUMBER")
issue_body = os.getenv("ISSUE_BODY")
repo_name = os.getenv("REPO_NAME")
github_token = os.getenv("GITHUB_TOKEN")

# Validação: Verifica se as informações necessárias foram capturadas
if not issue_body or not issue_number or not repo_name or not github_token:
    print("Erro: Informações da issue não foram capturadas corretamente.")
    exit(1)

# Expressões regulares para extrair queries e nome da tabela final
query_pattern = re.compile(r"Query \d+:\s*(.*?)(?=\nQuery \d+:|\nNome da tabela final:|$)", re.DOTALL)
table_pattern = re.compile(r"Nome da tabela final:\s*(\S+)")

# Extração das queries e do nome da tabela final
queries = query_pattern.findall(issue_body)
table_name_match = table_pattern.search(issue_body)
table_name = table_name_match.group(1) if table_name_match else "tabela_final"

# Validação: Pelo menos uma query deve ser fornecida
if not queries:
    print("Erro: Nenhuma query foi encontrada na issue.")
    exit(1)

# União das queries usando UNION ALL
final_query = " UNION ALL ".join([f"({q.strip()})" for q in queries])

# Construção da query final
final_query = f"CREATE TABLE {table_name} AS\n{final_query};"

# Log das operações
log_message = f"""
### 🔍 Processamento da Issue #{issue_number}
- 📂 **Repositório:** {repo_name}
- 📊 **Número de Queries Unificadas:** {len(queries)}
- 🏷 **Nome da Tabela Final:** {table_name}
- 🛠 **Query Final Gerada:**
"""

# Gera um arquivo SQL para download
output_filename = f"{table_name}.sql"
with open(output_filename, "w") as file:
    file.write(final_query)

# Enviar comentário na issue com a query final usando http.client
comment_url = f"/repos/{repo_name}/issues/{issue_number}/comments"
headers = {
    "Authorization": f"token {github_token}",
    "Content-Type": "application/json"
}
data = {
    "body": log_message
}
json_data = json.dumps(data)

# Conectar ao GitHub API
conn = http.client.HTTPSConnection("api.github.com")
conn.request("POST", comment_url, body=json_data, headers=headers)

response = conn.getresponse()
if response.status == 201:
    print("✅ Comentário postado com sucesso!")
else:
    print(f"⚠️ Erro ao postar comentário: {response.read().decode()}")

conn.close()
