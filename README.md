# Automação de União de Queries SQL para Itau Unibanco

Este projeto implementa uma automação para unir até 4 queries SQL, padronizando as nomenclaturas de acordo com um glossário. A automação é executada via GitHub Actions quando uma issue é criada com o formato esperado. O resultado é uma query final unificada, gerada em um arquivo SQL, e um log detalhado do processamento.

## Funcionalidades

- **Extração de Dados da Issue:** Lê o corpo da issue e extrai as queries e o nome da tabela final.
- **Validação:** Verifica se pelo menos uma query foi fornecida.
- **União das Queries:** Encapsula cada query entre parênteses e une todas com `UNION ALL`.
- **Correção de Nomenclaturas:** Carrega um glossário (CSV) e renomeia as colunas da query final conforme o padrão esperado.
- **Logs Detalhados:** Gera logs de cada etapa do processamento.
- **Integração com GitHub Actions:** Ao abrir uma issue com o formato correto, o workflow executa o script Python e publica um comentário na issue com a query final gerada.

## Estrutura do Repositório

seu-repositorio/ ├── .github/ │ └── workflows/ │ └── process_query.yml # Workflow do GitHub Actions ├── glossario.csv # Glossário com as colunas: nome, abreviacao e tipo ├── process_query.py # Script Python que processa a issue e gera a query final └── issue_example.txt # Arquivo para testes locais (simula o corpo de uma issue)

markdown
Copiar
Editar

## Dependências

### No Ambiente Python

- **Python 3.x**  
- **Pandas:** Utilizado para manipulação do CSV do glossário e processamento dos dados.  
  - Instalação: `pip install pandas`

### GitHub Actions

- **actions/checkout@v3:** Realiza o checkout do repositório.
- **actions/setup-python@v4:** Configura o ambiente Python.
- **actions/github-script@v6:** Permite executar scripts JavaScript para interagir com a API do GitHub (publicar comentários na issue).

## Como Funciona

1. **Criação da Issue:**  
   O usuário cria uma issue com o seguinte formato:
Query 1: <conteúdo da query 1>

Query 2: <conteúdo da query 2>

Query 3: <conteúdo da query 3> (opcional)

Query 4: <conteúdo da query 4> (opcional)

Nome da tabela final: <nome_da_tabela>

markdown
Copiar
Editar

2. **Execução do Workflow:**  
Ao abrir a issue, o GitHub Actions dispara o workflow definido em `.github/workflows/process_query.yml`.  
Esse workflow:
- Faz o checkout do repositório.
- Configura o Python e instala o Pandas.
- Executa o script `process_query.py` para processar a issue.
- Publica um comentário na issue com a query final gerada.

3. **Resultado:**  
O script gera um arquivo SQL com o nome da tabela final (por exemplo, `tabela_final_exemplo.sql`) e um log detalhado (`log.txt`). O conteúdo final da query é publicado automaticamente como comentário na issue.

## Testando Localmente

Para testar a automação localmente:

1. Crie um arquivo chamado `issue_example.txt` contendo o mesmo formato de uma issue.
2. Execute o script:
```bash
python process_query.py
Verifique os arquivos gerados (output.sql, <nome_da_tabela>.sql e log.txt).

Considerações Finais
Glossário: Certifique-se de que o arquivo glossario.csv esteja corretamente formatado com as colunas: nome, abreviacao e tipo.

Ajustes: Este projeto pode ser ajustado conforme as necessidades específicas do Itau Unibanco ou integrando outras validações e funcionalidades.

Logs: Os logs detalhados ajudam a acompanhar cada etapa do processamento, facilitando a identificação de problemas.

Contribuições e sugestões são bem-vindas!