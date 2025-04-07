import os
import re
import logging
from typing import List, Tuple
import sqlparse
from github import Github

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QueryExtractor:
    """Classe responsável por extrair queries SQL de issues do GitHub."""
    
    def __init__(self, token: str):
        self.github = Github(token)
    
    def extract_queries_from_issue(self, repo_name: str, issue_number: int) -> List[str]:
        repo = self.github.get_repo(repo_name)
        issue = repo.get_issue(number=issue_number)
        
        logger.info(f"Processando issue #{issue_number}: {issue.title}")
        
        queries = []
        query_pattern = r"Query\s+\d+:\s*(SELECT.+?)(?=(?:Query\s+\d+:)|$)"

        matches = re.finditer(query_pattern, issue.body, re.DOTALL | re.IGNORECASE)
        for match in matches:
            query = match.group(1).strip()
            queries.append(query)
            logger.info(f"Extraída Query #{len(queries)}")
        
        pattern = r"```sql\s*([\s\S]+?)\s*```"
        sql_blocks = re.findall(pattern, issue.body, re.DOTALL)
        
        for block in sql_blocks:
            if re.search(r"Query\s+\d+:", block, re.IGNORECASE):
                continue
            if block.strip().upper().startswith("SELECT"):
                queries.append(block.strip())
                logger.info(f"Extraída Query #{len(queries)} de bloco SQL")
        
        if not queries:
            logger.warning("Nenhuma query encontrada na issue")
            
        return queries

class SQLProcessor:
    """Classe responsável por processar e unificar queries SQL."""
    
    def __init__(self):
        pass
    
    def parse_columns(self, query: str) -> List[Tuple[str, str]]:
        try:
            query = sqlparse.format(query, keyword_case='upper', reindent=True)
            select_match = re.search(r"SELECT\s+(.+?)\s+FROM", query, re.DOTALL | re.IGNORECASE)
            if not select_match:
                logger.error(f"Não foi possível encontrar cláusula SELECT na query: {query[:100]}...")
                return []
            
            select_clause = select_match.group(1)
            
            if '*' in select_clause.strip():
                logger.warning("A query contém SELECT *. Isso pode causar problemas na unificação.")
                return [('*', '*')]
            
            columns = []
            current_col = ""
            parenthesis_count = 0
            in_quotes = False
            
            for char in select_clause:
                if char == "'" and not in_quotes:
                    in_quotes = True
                    current_col += char
                elif char == "'" and in_quotes:
                    in_quotes = False
                    current_col += char
                elif char == '(' and not in_quotes:
                    parenthesis_count += 1
                    current_col += char
                elif char == ')' and not in_quotes:
                    parenthesis_count -= 1
                    current_col += char
                elif char == ',' and parenthesis_count == 0 and not in_quotes:
                    columns.append(current_col.strip())
                    current_col = ""
                else:
                    current_col += char
            
            if current_col.strip():
                columns.append(current_col.strip())
            
            result = []
            for col in columns:
                alias_match = re.search(r"\s+AS\s+([^\s,]+)$", col, re.IGNORECASE)
                if alias_match:
                    alias = alias_match.group(1).strip('"`')
                    column = col[:alias_match.start()].strip()
                else:
                    parts = [part for part in col.split() if part]
                    if len(parts) > 1 and not re.search(r"[\(\)]", parts[-1]):
                        column = " ".join(parts[:-1]).strip()
                        alias = parts[-1].strip('"`')
                    else:
                        column = col.strip()
                        if '.' in column and '(' not in column:
                            alias = column.split('.')[-1].strip('"`')
                        else:
                            alias = column.strip('"`')
                
                result.append((column, alias))
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao analisar colunas da query: {e}")
            return []
    
    def fix_simple_syntax_errors(self, query: str) -> str:
        query = query.strip()
        if not query.endswith(';'):
            query += ';'
        
        query = re.sub(r'\s+', ' ', query)
        
        common_mistakes = {
            r'SELETC\b': 'SELECT',
            r'FROM\s+FROM\b': 'FROM',
            r'WEHRE\b': 'WHERE',
            r'GROUPP\s+BY\b': 'GROUP BY',
            r'ORDER\s+BYY\b': 'ORDER BY',
            r'JOIN\s+JOIN\b': 'JOIN',
            r'INNER\s+INNER\b': 'INNER',
        }
        
        for mistake, correction in common_mistakes.items():
            query = re.sub(mistake, correction, query, flags=re.IGNORECASE)
        
        return query
    
    def unify_queries(self, queries: List[str]) -> str:
        if not queries:
            return ""
        
        fixed_queries = [self.fix_simple_syntax_errors(query) for query in queries]
        
        all_column_sets = []
        for i, query in enumerate(fixed_queries):
            columns = self.parse_columns(query)
            all_column_sets.append((i, columns))
            logger.info(f"Query {i+1}: {len(columns)} colunas encontradas")
        
        all_aliases = set()
        for _, columns in all_column_sets:
            for _, alias in columns:
                all_aliases.add(alias)
        
        logger.info(f"Total de colunas unificadas: {len(all_aliases)}")
        
        ctes = []
        union_queries = []
        
        for i, (query_idx, columns) in enumerate(all_column_sets):
            query = fixed_queries[query_idx]
            cte_name = f"cte{query_idx + 1}"
            
            query = query.rstrip(';')
            cte = f"{cte_name} AS (\n  {query}\n)"
            ctes.append(cte)
            
            column_aliases = {alias: col for col, alias in columns}
            union_columns = []
            
            for alias in sorted(all_aliases):
                if alias in column_aliases:
                    union_columns.append(column_aliases[alias])
                else:
                    union_columns.append(f"NULL AS {alias}")
            
            union_query = f"SELECT {', '.join(union_columns)} FROM {cte_name}"
            union_queries.append(union_query)
        
        with_clause = "WITH " + ",\n".join(ctes)
        union_clause = "\nUNION ALL\n".join(union_queries)
        
        final_query = f"{with_clause}\n\n-- Query final unificada\nSELECT * FROM (\n{union_clause}\n) AS unified_result;"
        
        return final_query

class GitHubIntegration:
    """Classe responsável pela integração com GitHub."""
    
    def __init__(self, token: str):
        self.github = Github(token)
    
    def post_query_to_issue(self, repo_name: str, issue_number: int, unified_query: str, 
                           log_messages: List[str]) -> None:
        repo = self.github.get_repo(repo_name)
        issue = repo.get_issue(number=issue_number)
        
        comment = "## 🤖 Query Unificada\n\n"
        
        if log_messages:
            comment += "### Logs de Processamento\n"
            for msg in log_messages:
                comment += f"- {msg}\n"
            comment += "\n"
        
        comment += "### Query SQL Unificada\n"
        comment += "```sql\n"
        comment += unified_query
        comment += "\n```\n"
        
        issue.create_comment(comment)
        logger.info(f"Comentário postado na issue #{issue_number}")
    
    def save_unified_query(self, repo_name: str, issue_number: int, 
                          unified_query: str) -> None:
        repo = self.github.get_repo(repo_name)
        base_branch = repo.default_branch
        new_branch = f"unified-query-issue-{issue_number}"
        
        try:
            repo.get_git_ref(f"heads/{new_branch}")
            logger.info(f"Branch {new_branch} já existe")
        except:
            try:
                sha = repo.get_branch(base_branch).commit.sha
                repo.create_git_ref(ref=f"refs/heads/{new_branch}", sha=sha)
                logger.info(f"Criado novo branch: {new_branch}")
            except Exception as e:
                logger.error(f"Falha ao criar branch: {e}")
                return
        
        file_path = f"queries/unified-query-issue-{issue_number}.sql"
        
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
            try:
                repo.create_file(
                    path=file_path,
                    message=f"Adicionar query unificada da issue #{issue_number}",
                    content=unified_query,
                    branch=new_branch
                )
                logger.info(f"Arquivo {file_path} criado")
            except Exception as e:
                logger.error(f"Erro ao criar arquivo: {e}")
                return
        
        try:
            pulls = repo.get_pulls(state="open", head=f"{repo.owner.login}:{new_branch}")
            if pulls.totalCount > 0:
                logger.info(f"PR já existe para branch {new_branch}")
                return
            
            pr = repo.create_pull(
                title=f"Query Unificada da Issue #{issue_number}",
                body=f"Este PR contém a query unificada da issue #{issue_number}.",
                head=new_branch,
                base=base_branch
            )
            logger.info(f"Criado PR #{pr.number}")
        except Exception as e:
            logger.error(f"Erro ao criar PR: {e}")

def main():
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
    
    log_capture = []
    
    try:
        log_capture.append(f"Processando issue #{issue_number} do repositório {repo_name}")
        queries = extractor.extract_queries_from_issue(repo_name, issue_number)
        
        if not queries:
            log_capture.append("❌ Nenhuma query SQL encontrada na issue")
            github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture)
            return
        
        log_capture.append(f"✅ Encontradas {len(queries)} queries para processamento")
        
        for i, query in enumerate(queries):
            fixed_query = processor.fix_simple_syntax_errors(query)
            columns = processor.parse_columns(fixed_query)
            column_names = [alias for _, alias in columns]
            log_capture.append(f"Query {i+1}: {len(columns)} colunas identificadas - {', '.join(column_names)}")
        
        log_capture.append("🔄 Unificando queries...")
        unified_query = processor.unify_queries(queries)
        
        if not unified_query:
            log_capture.append("❌ Falha ao unificar as queries")
            github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture)
            return
        
        log_capture.append("✅ Queries unificadas com sucesso")
        
        github_integration.post_query_to_issue(repo_name, issue_number, unified_query, log_capture)
        github_integration.save_unified_query(repo_name, issue_number, unified_query)
        
    except Exception as e:
        logger.error(f"Erro durante o processamento: {e}")
        log_capture.append(f"❌ Erro durante o processamento: {e}")
        github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture)

if __name__ == "__main__":
    main()