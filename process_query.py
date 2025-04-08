import os
import re
import logging
from typing import List, Tuple, Dict, Set
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
    
    # Definindo mapeamento básico de tipos SQL para ajudar na detecção de incompatibilidades
    SQL_TYPE_MAPPING = {
        'integer': {'int', 'integer', 'smallint', 'bigint', 'tinyint'},
        'decimal': {'decimal', 'numeric', 'float', 'double', 'real'},
        'string': {'varchar', 'char', 'text', 'string', 'nvarchar', 'nchar'},
        'date': {'date', 'datetime', 'timestamp', 'time'},
        'boolean': {'boolean', 'bool'},
        'binary': {'binary', 'varbinary', 'blob'},
    }
    
    def __init__(self):
        self.type_warnings = []
    
    def infer_column_type(self, column_expr: str) -> str:
        """Infere o tipo de uma expressão de coluna com base em padrões comuns."""
        expr_lower = column_expr.lower()
        
        # Verifica se é um valor constante
        if re.search(r"^\s*\d+\s*$", column_expr):
            return "integer"
        elif re.search(r"^\s*\d+\.\d+\s*$", column_expr):
            return "decimal"
        elif re.search(r"^\s*'.*'\s*$", column_expr):
            return "string"
        
        # Verifica funções de agregação comuns
        if re.search(r"count\s*\(", expr_lower):
            return "integer"
        elif re.search(r"sum\s*\(", expr_lower):
            return "decimal" 
        elif re.search(r"avg\s*\(", expr_lower):
            return "decimal"
        elif re.search(r"min\s*\(|max\s*\(", expr_lower):
            return "unknown"  # depende do tipo da coluna interna
        
        # Funções de data
        if any(func in expr_lower for func in ['date', 'current_date', 'getdate', 'now']):
            return "date"
            
        # Funções de string
        if any(func in expr_lower for func in ['concat', 'substring', 'trim', 'lower', 'upper']):
            return "string"
            
        # Funções de conversão
        cast_pattern = r"cast\s*\(.+\s+as\s+(\w+)"
        if re.search(cast_pattern, expr_lower):
            type_match = re.search(cast_pattern, expr_lower)
            if type_match:
                cast_type = type_match.group(1)
                for category, types in self.SQL_TYPE_MAPPING.items():
                    if any(t in cast_type for t in types):
                        return category
        
        # Caso não seja possível determinar o tipo
        return "unknown"
    
    def are_types_compatible(self, type1: str, type2: str) -> bool:
        """Verifica se dois tipos SQL são compatíveis para UNION."""
        if type1 == type2:
            return True
            
        if type1 == "unknown" or type2 == "unknown":
            return True
            
        # Números são geralmente compatíveis entre si
        if type1 in ['integer', 'decimal'] and type2 in ['integer', 'decimal']:
            return True
            
        # Strings e datas geralmente têm conversão implícita
        if type1 in ['string', 'date'] and type2 in ['string', 'date']:
            return True
            
        return False
    
    def parse_columns(self, query: str) -> List[Tuple[str, str, str]]:
        """
        Analisa as colunas de uma query e tenta inferir seus tipos.
        Retorna uma lista de tuplas (expressão, alias, tipo inferido).
        """
        try:
            query = sqlparse.format(query, keyword_case='upper', reindent=True)
            select_pattern = r"SELECT\s+(.+?)\s+FROM"
            select_match = re.search(select_pattern, query, re.DOTALL | re.IGNORECASE)
            if not select_match:
                logger.error(f"Não foi possível encontrar cláusula SELECT na query: {query[:100]}...")
                return []
            
            select_clause = select_match.group(1)
            
            if '*' in select_clause.strip():
                logger.warning("A query contém SELECT *. Isso pode causar problemas na unificação.")
                return [('*', '*', 'unknown')]
            
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
            alias_pattern = r"\s+AS\s+([^\s,]+)$"
            
            for col in columns:
                alias_match = re.search(alias_pattern, col, re.IGNORECASE)
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
                
                inferred_type = self.infer_column_type(column)
                result.append((column, alias, inferred_type))
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao analisar colunas da query: {e}")
            return []
    
    def extract_query_components(self, query: str) -> Dict[str, str]:
        """
        Extrai os componentes principais de uma query SQL (SELECT, FROM, WHERE, GROUP BY, etc.).
        Retorna um dicionário com os componentes identificados.
        """
        components = {
            'SELECT': '',
            'FROM': '',
            'WHERE': '',
            'GROUP BY': '',
            'HAVING': '',
            'ORDER BY': '',
            'LIMIT': ''
        }
        
        # Formatar a query para facilitar análise
        formatted_query = sqlparse.format(query, keyword_case='upper')
        
        # Padrões regex para cada componente
        select_pattern = r"SELECT\s+(.+?)(?:\s+FROM\s+|$)"
        from_pattern = r"FROM\s+(.+?)(?:\s+WHERE\s+|\s+GROUP\s+BY\s+|\s+HAVING\s+|\s+ORDER\s+BY\s+|\s+LIMIT\s+|$)"
        where_pattern = r"WHERE\s+(.+?)(?:\s+GROUP\s+BY\s+|\s+HAVING\s+|\s+ORDER\s+BY\s+|\s+LIMIT\s+|$)"
        group_by_pattern = r"GROUP\s+BY\s+(.+?)(?:\s+HAVING\s+|\s+ORDER\s+BY\s+|\s+LIMIT\s+|$)"
        having_pattern = r"HAVING\s+(.+?)(?:\s+ORDER\s+BY\s+|\s+LIMIT\s+|$)"
        order_by_pattern = r"ORDER\s+BY\s+(.+?)(?:\s+LIMIT\s+|$)"
        limit_pattern = r"LIMIT\s+(.+?)$"
        
        # Extrair cada componente usando expressões regulares
        select_match = re.search(select_pattern, formatted_query, re.DOTALL)
        if select_match:
            components['SELECT'] = select_match.group(1).strip()
        
        from_match = re.search(from_pattern, formatted_query, re.DOTALL)
        if from_match:
            components['FROM'] = from_match.group(1).strip()
        
        where_match = re.search(where_pattern, formatted_query, re.DOTALL)
        if where_match:
            components['WHERE'] = where_match.group(1).strip()
        
        group_by_match = re.search(group_by_pattern, formatted_query, re.DOTALL)
        if group_by_match:
            components['GROUP BY'] = group_by_match.group(1).strip()
        
        having_match = re.search(having_pattern, formatted_query, re.DOTALL)
        if having_match:
            components['HAVING'] = having_match.group(1).strip()
        
        order_by_match = re.search(order_by_pattern, formatted_query, re.DOTALL)
        if order_by_match:
            components['ORDER BY'] = order_by_match.group(1).strip()
        
        limit_match = re.search(limit_pattern, formatted_query)
        if limit_match:
            components['LIMIT'] = limit_match.group(1).strip()
        
        return components
    
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
            r'HAVINGG\b': 'HAVING',
            r'WHERRE\b': 'WHERE',
        }
        
        for mistake, correction in common_mistakes.items():
            query = re.sub(mistake, correction, query, flags=re.IGNORECASE)
        
        return query
    
    def check_type_compatibility(self, all_column_sets: List[Tuple[int, List[Tuple[str, str, str]]]]) -> List[str]:
        """
        Verifica a compatibilidade de tipos entre colunas de diferentes queries.
        Retorna uma lista de alertas sobre possíveis incompatibilidades.
        """
        warnings = []
        
        # Agrupar colunas por alias
        alias_to_types = {}
        for query_idx, columns in all_column_sets:
            for _, alias, inferred_type in columns:
                if alias not in alias_to_types:
                    alias_to_types[alias] = []
                alias_to_types[alias].append((query_idx, inferred_type))
        
        # Verificar compatibilidade de tipos para cada alias
        for alias, type_info in alias_to_types.items():
            # Ignorar casos onde só há uma query usando o alias
            if len(type_info) <= 1:
                continue
                
            types = [t for _, t in type_info]
            query_indices = [idx for idx, _ in type_info]
            
            # Verificar incompatibilidades
            for i, type1 in enumerate(types):
                for j, type2 in enumerate(types[i+1:], i+1):
                    if not self.are_types_compatible(type1, type2):
                        warnings.append(
                            f"⚠️ Possível incompatibilidade de tipos para coluna '{alias}': "
                            f"Query {query_indices[i]+1} usa tipo '{type1}', "
                            f"Query {query_indices[j]+1} usa tipo '{type2}'"
                        )
        
        return warnings
    
    def unify_queries(self, queries: List[str]) -> str:
        if not queries:
            return ""
        self.type_warnings = []  # Limpar avisos anteriores
        fixed_queries = [self.fix_simple_syntax_errors(query) for query in queries]
        all_column_sets = []
        query_components = []
        for i, query in enumerate(fixed_queries):
            components = self.extract_query_components(query)
            query_components.append(components)
            columns = self.parse_columns(query)
            all_column_sets.append((i, columns))
            logger.info(f"Query {i+1}: {len(columns)} colunas encontradas")
            if components['GROUP BY'] or components['HAVING']:
                logger.info(f"Query {i+1} contém cláusulas GROUP BY/HAVING")
        type_warnings = self.check_type_compatibility(all_column_sets)
        self.type_warnings.extend(type_warnings)
        all_aliases = set()
        for _, columns in all_column_sets:
            for _, alias, _ in columns:
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
            column_dict = {alias: (col, typ) for col, alias, typ in columns}
            union_columns = []
            for alias in sorted(all_aliases):
                if alias in column_dict:
                    col, _ = column_dict[alias]
                    union_columns.append(col)
                else:
                    union_columns.append(f"NULL AS {alias}")
            union_query = f"SELECT {', '.join(union_columns)} FROM {cte_name}"
            union_queries.append(union_query)
        with_clause = "WITH " + ",\n".join(ctes)
        union_clause = "\nUNION ALL\n".join(union_queries)
        final_query = (
            with_clause + "\n"
            "-- Query final unificada\n"
            "SELECT * FROM (\n" + union_clause + "\n) AS unified_result;"
        )
        return final_query

class GitHubIntegration:
    """Classe responsável pela integração com GitHub."""

    def __init__(self, token: str):
        self.github = Github(token)

    def post_query_to_issue(self, repo_name: str, issue_number: int, unified_query: str, 
                            log_messages: List[str], type_warnings: List[str] = None) -> None:
        repo = self.github.get_repo(repo_name)
        issue = repo.get_issue(number=issue_number)
        comment = "## 🤖 Query Unificada\n"
        if type_warnings:
            comment += "### ⚠️ Alertas de Compatibilidade\n"
            for warning in type_warnings:
                comment += warning + "\n"
            comment += "\n"
        if log_messages:
            comment += "### Logs de Processamento\n"
            for msg in log_messages:
                comment += msg + "\n"
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
            
            # Identificar cláusulas GROUP BY e HAVING
            components = processor.extract_query_components(fixed_query)
            if components['GROUP BY']:
                log_capture.append(f"ℹ️ Query {i+1} contém GROUP BY: {components['GROUP BY']}")
            if components['HAVING']:
                log_capture.append(f"ℹ️ Query {i+1} contém HAVING: {components['HAVING']}")
                
            column_info = []
            for col, alias, col_type in columns:
                column_info.append(f"{alias} ({col_type})")
            
            log_capture.append(f"Query {i+1}: {len(columns)} colunas identificadas - {', '.join(column_info)}")
        
        log_capture.append("🔄 Unificando queries...")
        unified_query = processor.unify_queries(queries)
        
        if not unified_query:
            log_capture.append("❌ Falha ao unificar as queries")
            github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture)
            return
        
        log_capture.append("✅ Queries unificadas com sucesso")
        
        github_integration.post_query_to_issue(repo_name, issue_number, unified_query, log_capture, processor.type_warnings)
        github_integration.save_unified_query(repo_name, issue_number, unified_query)
        
    except Exception as e:
        logger.error(f"Erro durante o processamento: {e}")
        log_capture.append(f"❌ Erro durante o processamento: {e}")
        github_integration.post_query_to_issue(repo_name, issue_number, "", log_capture)

if __name__ == "__main__":
    main()