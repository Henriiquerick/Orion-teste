import os
import re
import logging
from typing import List, Dict, Set, Tuple
import sqlparse
from github import Github
import json

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QueryExtractor:
    """Classe responsável por extrair queries SQL de issues do GitHub."""
    
    def __init__(self, token: str):
        """
        Inicializa o extrator de queries.
        
        Args:
            token: Token de autenticação do GitHub
        """
        self.github = Github(token)
    
    def extract_queries_from_issue(self, repo_name: str, issue_number: int) -> List[str]:
        """
        Extrai queries SQL de uma issue do GitHub.
        
        Args:
            repo_name: Nome do repositório no formato 'dono/repositorio'
            issue_number: Número da issue
            
        Returns:
            Lista de queries SQL extraídas
        """
        repo = self.github.get_repo(repo_name)
        issue = repo.get_issue(number=issue_number)
        
        logger.info(f"Processando issue #{issue_number}: {issue.title}")
        
        # Extrair queries do corpo da issue
        queries = []
        query_pattern = r"Query\s+\d+:\s+(SELECT.+?)((?=Query\s+\d+:)|$)"
        
        # Usando regex para encontrar todas as queries no formato "Query X: SELECT..."
        matches = re.finditer(query_pattern, issue.body, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            query = match.group(1).strip()
            queries.append(query)
            logger.info(f"Extraída Query #{len(queries)}")
        
        # Considerar também o formato SQL code block
        sql_blocks = re.findall(r"```sql\s*(.+?)\s*```", issue.body, re.DOTALL)
        for block in sql_blocks:
            # Verificar se o bloco contém múltiplas queries
            if re.search(r"Query\s+\d+:", block, re.IGNORECASE):
                # Já extraímos usando o padrão anterior
                continue
            else:
                # Considerar o bloco inteiro como uma única query
                if block.strip().upper().startswith("SELECT"):
                    queries.append(block.strip())
                    logger.info(f"Extraída Query #{len(queries)} de bloco SQL")
        
        if not queries:
            logger.warning("Nenhuma query encontrada na issue")
            
        return queries

class SQLProcessor:
    """Classe responsável por processar e unificar queries SQL."""
    
    def __init__(self):
        """Inicializa o processador SQL."""
        pass
    
    def parse_columns(self, query: str) -> List[Tuple[str, str]]:
        """
        Analisa a query SQL e extrai as colunas e seus aliases.
        
        Args:
            query: Query SQL para análise
            
        Returns:
            Lista de tuplas (coluna, alias)
        """
        try:
            # Formatar a query para evitar problemas de parsing
            query = sqlparse.format(query, keyword_case='upper', reindent=True)
            
            # Extrair a parte SELECT da query
            select_match = re.search(r"SELECT\s+(.+?)\s+FROM", query, re.DOTALL | re.IGNORECASE)
            if not select_match:
                logger.error(f"Não foi possível encontrar cláusula SELECT na query: {query[:100]}...")
                return []
            
            select_clause = select_match.group(1)
            
            # Dividir as colunas
            columns = []
            
            # Caso especial: SELECT * 
            if '*' in select_clause:
                logger.warning("A query contém SELECT *. Isso pode causar problemas na unificação.")
                return [('*', '*')]
            
            # Processar colunas
            current_col = ""
            parenthesis_count = 0
            
            for char in select_clause:
                if char == '(' and not current_col.endswith("'"):
                    parenthesis_count += 1
                    current_col += char
                elif char == ')' and not current_col.endswith("'"):
                    parenthesis_count -= 1
                    current_col += char
                elif char == ',' and parenthesis_count == 0:
                    # Final de uma coluna
                    columns.append(current_col.strip())
                    current_col = ""
                else:
                    current_col += char
            
            # Adicionar a última coluna
            if current_col.strip():
                columns.append(current_col.strip())
            
            # Processar cada coluna para extrair nome e alias
            result = []
            for col in columns:
                # Verificar se há um alias explícito
                alias_match = re.search(r"AS\s+([^\s,]+)$", col, re.IGNORECASE)
                
                if alias_match:
                    alias = alias_match.group(1).strip('"`')
                    # Extrair a parte da coluna sem o alias
                    column = col[:alias_match.start()].strip()
                else:
                    # Se não há alias explícito, verificar se há alias implícito
                    parts = col.split()
                    if len(parts) > 1 and not re.search(r"[\(\)]", parts[-1]):
                        column = ' '.join(parts[:-1]).strip()
                        alias = parts[-1].strip('"`')
                    else:
                        # Sem alias, usar o nome da coluna
                        column = col.strip()
                        # Verificar se é uma expressão ou nome simples
                        if '.' in column and '(' not in column:
                            # É um nome qualificado, pegar apenas a última parte
                            alias = column.split('.')[-1].strip('"`')
                        else:
                            # É um nome simples ou expressão
                            alias = column.strip('"`')
                
                result.append((column, alias))
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao analisar colunas da query: {str(e)}")
            return []
    
    def fix_simple_syntax_errors(self, query: str) -> str:
        """
        Tenta corrigir erros simples de sintaxe na query.
        
        Args:
            query: Query SQL com possíveis erros
            
        Returns:
            Query SQL corrigida
        """
        # Corrigir ponto e vírgula no final
        query = query.strip()
        if not query.endswith(';'):
            query += ';'
        
        # Corrigir espaços extras entre palavras-chave
        query = re.sub(r'\s+', ' ', query)
        
        # Corrigir palavras-chave comuns mal escritas
        common_mistakes = {
            r'\bSELETC\b': 'SELECT',
            r'\bFROM\s+FROM\b': 'FROM',
            r'\bWEHRE\b': 'WHERE',
            r'\bGROUPP\s+BY\b': 'GROUP BY',
            r'\bORDER\s+BYY\b': 'ORDER BY',
            r'\bJOIN\s+JOIN\b': 'JOIN',
            r'\bINNER\s+INNER\b': 'INNER',
        }
        
        for mistake, correction in common_mistakes.items():
            query = re.sub(mistake, correction, query, flags=re.IGNORECASE)
        
        return query
    
    def unify_queries(self, queries: List[str]) -> str:
        """
        Unifica múltiplas queries SQL usando CTEs e UNION ALL.
        
        Args:
            queries: Lista de queries SQL a serem unificadas
            
        Returns:
            Query SQL unificada com CTEs e UNION ALL
        """
        if not queries:
            return ""
        
        # Corrigir possíveis erros sintáticos
        fixed_queries = [self.fix_simple_syntax_errors(query) for query in queries]
        
        # Extrair colunas de cada query
        all_column_sets = []
        for i, query in enumerate(fixed_queries):
            columns = self.parse_columns(query)
            all_column_sets.append((i, columns))
            logger.info(f"Query {i+1}: {len(columns)} colunas encontradas")
        
        # Construir um conjunto unificado de todas as colunas (por alias)
        all_aliases = set()
        for _, columns in all_column_sets:
            for _, alias in columns:
                all_aliases.add(alias)
        
        logger.info(f"Total de colunas unificadas: {len(all_aliases)}")
        
        # Construir CTEs
        ctes = []
        union_queries = []
        
        for i, (query_idx, columns) in enumerate(all_column_sets):
            query = fixed_queries[query_idx]
            cte_name = f"cte{query_idx+1}"
            
            # Remover ponto e vírgula do final para incorporar na CTE
            query = query.rstrip(';')
            
            # Criar a CTE
            cte = f"{cte_name} AS (\n  {query}\n)"
            ctes.append(cte)
            
            # Construir a parte do UNION para esta query
            column_aliases = {alias: col for col, alias in columns}
            union_columns = []
            
            for alias in sorted(all_aliases):
                if alias in column_aliases:
                    union_columns.append(column_aliases[alias])
                else:
                    union_columns.append(f"NULL AS {alias}")
            
            union_query = f"SELECT {', '.join(union_columns)} FROM {cte_name}"
            union_queries.append(union_query)
        
        # Construir a query final
        with_clause = "WITH " + ",\n".join(ctes)
        union_clause = "\nUNION ALL\n".join(union_queries)
        
        final_query = f"{with_clause}\n\n-- Query final unificada\nSELECT * FROM (\n{union_clause}\n) AS unified_result;"
        
        return final_query

class GitHubIntegration:
    """Classe responsável pela integração com GitHub."""
    
    def __init__(self, token: str):
        """
        Inicializa a integração com GitHub.
        
        Args:
            token: Token de autenticação do GitHub
        """
        self.github = Github(token)
    
    def post_query_to_issue(self, repo_name: str, issue_number: int, unified_query: str, 
                           log_messages: List[str]) -> None:
        """
        Posta a query unificada como comentário na issue.
        
        Args:
            repo_name: Nome do repositório
            issue_number: Número da issue
            unified_query: Query SQL unificada
            log_messages: Mensagens de log para incluir no comentário
        """
        repo = self.github.get_repo(repo_name)
        issue = repo.get_issue(number=issue_number)
        
        comment = "## 🤖 Query Unificada\n\n"
        
        # Adicionar mensagens de log
        if log_messages:
            comment += "### Logs de Processamento\n"
            for msg in log_messages:
                comment += f"- {msg}\n"
            comment += "\n"
        
        # Adicionar a query unificada
        comment += "### Query SQL Unificada\n"
        comment += "```sql\n"
        comment += unified_query
        comment += "\n```\n"
        
        issue.create_comment(comment)
        logger.info(f"Comentário postado na issue #{issue_number}")
    
    def save_unified_query(self, repo_name: str, issue_number: int, 
                          unified_query: str) -> None:
        """
        Salva a query unificada como um arquivo no repositório.
        
        Args:
            repo_name: Nome do repositório
            issue_number: Número da issue
            unified_query: Query SQL unificada
        """
        repo = self.github.get_repo(repo_name)
        
        # Criar um novo branch baseado no branch principal
        base_branch = repo.default_branch
        new_branch = f"unified-query-issue-{issue_number}"
        
        # Verificar se o branch já existe
        try:
            ref = repo.get_git_ref(f"heads/{new_branch}")
            # Se não lançar exceção, o branch existe
            logger.info(f"Branch {new_branch} já existe, usando-o")
        except:
            # Criar novo branch
            sha = repo.get_branch(base_branch).commit.sha
            repo.create_git_ref(ref=f"refs/heads/{new_branch}", sha=sha)
            logger.info(f"Criado novo branch: {new_branch}")
        
        # Nome do arquivo
        file_path = f"queries/unified-query-issue-{issue_number}.sql"
        
        # Verificar se o arquivo já existe
        try:
            contents = repo.get_contents(file_path, ref=new_branch)
            # Atualizar arquivo existente
            repo.update_file(
                path=file_path,
                message=f"Atualizar query unificada da issue #{issue_number}",
                content=unified_query,
                sha=contents.sha,
                branch=new_branch
            )
            logger.info(f"Arquivo {file_path} atualizado no branch {new_branch}")
        except:
            # Criar novo arquivo
            try:
                repo.create_file(
                    path=file_path,
                    message=f"Adicionar query unificada da issue #{issue_number}",
                    content=unified_query,
                    branch=new_branch
                )
                logger.info(f"Arquivo {file_path} criado no branch {new_branch}")
            except Exception as e:
                # Pode ser necessário criar o diretório
                logger.error(f"Erro ao criar arquivo: {str(e)}")
                return
        
        # Criar pull request se ainda não existir
        try:
            # Verificar se já existe um PR para este branch
            for pr in repo.get_pulls(state="open"):
                if pr.head.ref == new_branch:
                    logger.info(f"PR já existe: #{pr.number}")
                    return
            
            # Criar novo PR
            pr = repo.create_pull(
                title=f"Query Unificada da Issue #{issue_number}",
                body=f"Este PR contém a query unificada solicitada na issue #{issue_number}.\n\n"
                     f"A query foi processada automaticamente pelo Query Unifier.",
                head=new_branch,
                base=base_branch
            )
            logger.info(f"Criado PR #{pr.number} para o branch {new_branch}")
        except Exception as e:
            logger.error(f"Erro ao criar PR: {str(e)}")

def main():
    """Função principal para processar issues do GitHub."""
    # Obter variáveis de ambiente
    github_token = os.environ.get("GITHUB_TOKEN")
    repo_name = os.environ.get("GITHUB_REPOSITORY")
    issue_number = os.environ.get("ISSUE_NUMBER")
    
    # Verificar se as variáveis foram fornecidas
    if not all([github_token, repo_name, issue_number]):
        logger.error("Variáveis de ambiente necessárias não foram fornecidas.")
        return
    
    try:
        issue_number = int(issue_number)
    except ValueError:
        logger.error(f"Número da issue inválido: {issue_number}")
        return
    
    # Inicializar as classes
    extractor = QueryExtractor(github_token)
    processor = SQLProcessor()
    github_integration = GitHubIntegration(github_token)
    
    # Capturar logs para reportar na issue
    log_capture = []
    
    try:
        # Extrair queries da issue
        log_capture.append(f"Processando issue #{issue_number} do repositório {repo_name}")
        queries = extractor.extract_queries_from_issue(repo_name, issue_number)
        
        if not queries:
            log_capture.append("❌ Nenhuma query SQL encontrada na issue")
            github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture)
            return
        
        log_capture.append(f"✅ Encontradas {len(queries)} queries para processamento")
        
        # Processar e unificar as queries
        for i, query in enumerate(queries):
            fixed_query = processor.fix_simple_syntax_errors(query)
            columns = processor.parse_columns(fixed_query)
            column_names = [alias for _, alias in columns]
            log_capture.append(f"Query {i+1}: {len(columns)} colunas identificadas - {', '.join(column_names)}")
        
        # Unificar as queries
        log_capture.append("🔄 Unificando queries...")
        unified_query = processor.unify_queries(queries)
        
        if not unified_query:
            log_capture.append("❌ Falha ao unificar as queries")
            github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture)
            return
        
        log_capture.append("✅ Queries unificadas com sucesso")
        
        # Postar a query unificada na issue
        github_integration.post_query_to_issue(repo_name, issue_number, unified_query, log_capture)
        
        # Salvar a query unificada no repositório
        github_integration.save_unified_query(repo_name, issue_number, unified_query)
        
    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")
        log_capture.append(f"❌ Erro durante o processamento: {str(e)}")
        github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture)

if __name__ == "__main__":
    main()