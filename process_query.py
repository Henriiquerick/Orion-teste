import os
import re
import logging
from typing import List, Dict, Tuple
import sqlparse
from github import Github

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QueryExtractor:
    """Classe responsável por extrair queries SQL e metadados de issues do GitHub."""
    
    def __init__(self, token: str):
        self.github = Github(token)
    
    def extract_queries_from_issue(self, repo_name: str, issue_number: int) -> Tuple[List[str], str]:
        repo = self.github.get_repo(repo_name)
        issue = repo.get_issue(number=issue_number)
        
        logger.info(f"Processando issue #{issue_number}: {issue.title}")
        
        table_name_match = re.search(r"\*\*Nome da Tabela Final:\*\*\s*(.+)", issue.body)
        table_name = table_name_match.group(1).strip() if table_name_match else "unified_result"
        logger.info(f"Nome da tabela final extraído: {table_name}")
        
        queries = []
        sql_blocks = re.findall(r"```sql\s*(.+?)\s*```", issue.body, re.DOTALL)
        
        for i, block in enumerate(sql_blocks, 1):
            if block.strip().upper().startswith("SELECT"):
                queries.append(block.strip())
                logger.info(f"Extraída Query #{i} de bloco SQL")
        
        if not queries:
            logger.warning("Nenhuma query encontrada na issue")
            
        return queries, table_name

class SQLProcessor:
    """Classe responsável por processar e unificar queries SQL."""
    
    def __init__(self):
        self.log_capture = []
    
    def parse_columns(self, query: str) -> List[Tuple[str, str]]:
        try:
            parsed = sqlparse.parse(query)[0]
            select_idx = next(i for i, t in enumerate(parsed.tokens) 
                            if t.ttype is sqlparse.tokens.DML and t.value.upper() == 'SELECT')
            columns = []
            for token in parsed.tokens[select_idx + 1:]:
                if token.value.upper() == 'FROM':
                    break
                if isinstance(token, sqlparse.sql.TokenList):
                    for col in token.get_identifiers():
                        col_str = col.value
                        alias = col.get_alias() or col.get_name() or col_str.split('.')[-1].strip('"`')
                        columns.append((col_str, alias))
            if not columns:
                logger.error(f"Não foi possível extrair colunas da query: {query[:100]}...")
            return columns
        except Exception as e:
            logger.error(f"Erro ao analisar colunas da query: {str(e)}")
            return []
    
    def fix_simple_syntax_errors(self, query: str) -> str:
        query = query.strip()
        if not query.endswith(';'):
            query += ';'
        query = re.sub(r'\s+', ' ', query)
        common_mistakes = {
            r'\bSELETC\b': 'SELECT',
            r'\bFROM\s+FROM\b': 'FROM',
            r'\bWEHRE\b': 'WHERE',
            r'\bGROUPP\s+BY\b': 'GROUP BY',
            r'\bORDER\s+BYY\b': 'ORDER BY',
        }
        for mistake, correction in common_mistakes.items():
            query = re.sub(mistake, correction, query, flags=re.IGNORECASE)
        return query
    
    def unify_queries(self, queries: List[str], table_name: str) -> str:
        if not queries:
            return ""
        
        fixed_queries = [self.fix_simple_syntax_errors(query) for query in queries]
        all_column_sets = []
        self.log_capture = []
        
        for i, query in enumerate(fixed_queries):
            columns = self.parse_columns(query)
            if not columns or any(col == '*' for col, _ in columns):
                self.log_capture.append(f"Query {i+1} ignorada: contém SELECT * ou é inválida")
                continue
            all_column_sets.append((i, columns))
            column_names = [alias for _, alias in columns]
            self.log_capture.append(f"Query {i+1}: {len(columns)} colunas - {', '.join(column_names)}")
        
        if not all_column_sets:
            self.log_capture.append("Nenhuma query válida para unificação")
            return ""
        
        all_aliases = set()
        for _, columns in all_column_sets:
            all_aliases.update(alias for _, alias in columns)
        
        ctes = []
        union_queries = []
        
        for i, (query_idx, columns) in enumerate(all_column_sets):
            cte_name = f"cte{query_idx+1}"
            query = fixed_queries[query_idx].rstrip(';')
            ctes.append(f"{cte_name} AS (\n  {query}\n)")
            
            column_aliases = {alias: col for col, alias in columns}
            union_columns = []
            for alias in sorted(all_aliases):
                if alias in column_aliases:
                    union_columns.append(column_aliases[alias])
                else:
                    union_columns.append(f"NULL AS {alias}")
                    self.log_capture.append(f"Coluna '{alias}' adicionada como NULL na {cte_name}")
            
            union_queries.append(f"SELECT {', '.join(union_columns)} FROM {cte_name}")
        
        final_query = (
            f"WITH {',\n'.join(ctes)}\n\n"
            f"-- Query final unificada\n"
            f"SELECT * FROM (\n{' UNION ALL\n'.join(union_queries)}\n) AS {table_name};"
        )
        
        return final_query

class GitHubIntegration:
    """Classe responsável pela integração com GitHub."""
    
    def __init__(self, token: str):
        self.github = Github(token)
    
    def post_initial_comment(self, repo_name: str, issue_number: int) -> None:
        repo = self.github.get_repo(repo_name)
        issue = repo.get_issue(number=issue_number)
        
        comment = (
            "## 🚀 Processamento Iniciado\n\n"
            "Olá! O processamento da sua solicitação de unificação de queries começou. "
            "Você pode acompanhar o andamento:\n"
            "- **Aqui na issue**: Fique de olho nos comentários para logs e resultados.\n"
            "- **GitHub Actions**: Veja o progresso em tempo real na aba 'Actions' deste repositório.\n\n"
            "Em breve, postaremos a query unificada e criaremos um PR com o resultado!"
        )
        
        issue.create_comment(comment)
        logger.info(f"Comentário inicial postado na issue #{issue_number}")
    
    def post_query_to_issue(self, repo_name: str, issue_number: int, unified_query: str, 
                           log_messages: List[str], table_name: str) -> None:
        """
        Posta a query unificada como conteúdo de um arquivo .sql no comentário da issue.
        
        Args:
            repo_name: Nome do repositório
            issue_number: Número da issue
            unified_query: Query SQL unificada
            log_messages: Mensagens de log para incluir no comentário
            table_name: Nome da tabela final para nomear o arquivo
        """
        repo = self.github.get_repo(repo_name)
        issue = repo.get_issue(number=issue_number)
        
        comment = "## 🤖 Resultado da Unificação\n\n"
        
        if log_messages:
            comment += "### Logs de Processamento\n"
            for msg in log_messages:
                comment += f"- {msg}\n"
            comment += "\n"
        
        # Adicionar o conteúdo do arquivo .sql
        comment += f"### Arquivo `{table_name}-issue-{issue_number}.sql`\n"
        comment += "Abaixo está o conteúdo do arquivo `.sql` gerado:\n\n"
        comment += "```sql\n"
        comment += unified_query if unified_query else "-- Nenhuma query unificada gerada"
        comment += "\n```\n"
        comment += f"\nEste arquivo foi salvo no repositório em `queries/{table_name}-issue-{issue_number}.sql` e está em um PR para revisão."
        
        issue.create_comment(comment)
        logger.info(f"Comentário com arquivo .sql postado na issue #{issue_number}")
    
    def save_unified_query(self, repo_name: str, issue_number: int, 
                          unified_query: str, table_name: str) -> None:
        repo = self.github.get_repo(repo_name)
        base_branch = repo.default_branch
        new_branch = f"unified-query-issue-{issue_number}"
        
        try:
            ref = repo.get_git_ref(f"heads/{new_branch}")
            logger.info(f"Branch {new_branch} já existe")
        except:
            sha = repo.get_branch(base_branch).commit.sha
            repo.create_git_ref(ref=f"refs/heads/{new_branch}", sha=sha)
            logger.info(f"Criado novo branch: {new_branch}")
        
        file_path = f"queries/{table_name}-issue-{issue_number}.sql"
        
        try:
            contents = repo.get_contents(file_path, ref=new_branch)
            repo.update_file(
                path=file_path,
                message=f"Atualizar query unificada da issue #{issue_number}",
                content=unified_query,
                sha=contents.sha,
                branch=new_branch
            )
            logger.info(f"Arquivo {file_path} atualizado")
        except:
            repo.create_file(
                path=file_path,
                message=f"Adicionar query unificada da issue #{issue_number}",
                content=unified_query,
                branch=new_branch
            )
            logger.info(f"Arquivo {file_path} criado")
        
        for pr in repo.get_pulls(state="open"):
            if pr.head.ref == new_branch:
                logger.info(f"PR já existe: #{pr.number}")
                return
        
        pr = repo.create_pull(
            title=f"Query Unificada da Issue #{issue_number} - {table_name}",
            body=f"Query unificada para a tabela '{table_name}' da issue #{issue_number}.",
            head=new_branch,
            base=base_branch
        )
        logger.info(f"Criado PR #{pr.number}")

def main():
    """Função principal para processar issues do GitHub."""
    github_token = os.environ.get("GITHUB_TOKEN")
    repo_name = os.environ.get("GITHUB_REPOSITORY")
    issue_number = os.environ.get("ISSUE_NUMBER")
    
    if not all([github_token, repo_name, issue_number]):
        logger.error("Variáveis de ambiente necessárias não foram fornecidas.")
        return
    
    try:
        issue_number = int(issue_number)
    except ValueError:
        logger.error(f"Número da issue inválido: {issue_number}")
        return
    
    extractor = QueryExtractor(github_token)
    processor = SQLProcessor()
    github_integration = GitHubIntegration(github_token)
    
    github_integration.post_initial_comment(repo_name, issue_number)
    
    log_capture = [f"Processando issue #{issue_number} do repositório {repo_name}"]
    
    try:
        queries, table_name = extractor.extract_queries_from_issue(repo_name, issue_number)
        
        if not queries:
            log_capture.append("❌ Nenhuma query SQL encontrada na issue")
            github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture, table_name)
            return
        
        log_capture.append(f"✅ Encontradas {len(queries)} queries para processamento")
        
        unified_query = processor.unify_queries(queries, table_name)
        log_capture.extend(processor.log_capture)
        
        if not unified_query:
            log_capture.append("❌ Falha ao unificar as queries")
            github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture, table_name)
            return
        
        log_capture.append("✅ Queries unificadas com sucesso")
        
        github_integration.post_query_to_issue(repo_name, issue_number, unified_query, log_capture, table_name)
        github_integration.save_unified_query(repo_name, issue_number, unified_query, table_name)
        
    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")
        log_capture.append(f"❌ Erro: {str(e)}")
        github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture, table_name)

if __name__ == "__main__":
    main()