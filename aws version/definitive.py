"""
Orion SQL Query Unifier

Este módulo unifica múltiplas consultas SQL do Amazon Athena em uma única consulta otimizada.
Ele analisa schemas de tabelas, identifica incompatibilidades de tipos de dados,
e gera uma única consulta SQL otimizada para o Athena.

Autor: Seu Nome
Data: 14 de abril de 2025
"""

import time
import boto3
import tkinter as tk
from tkinter import ttk, messagebox
from typing import List, Dict, Any, Callable, Optional, Tuple, Set
from botocore.exceptions import ClientError


class OrionQueryUnifier:
    """Classe principal para o Orion SQL Query Unifier."""
    
    # Constantes de configuração
    DEFAULT_REGION = "sa-east-1"
    WORKGROUP = "analytics-workgroup-v3"
    S3_RESULTS_BUCKET = "analytics-query-result-athena-sa-east-1-261034348095"
    
    def __init__(self):
        """Inicializa o objeto OrionQueryUnifier."""
        self.athena_client = None
        self.logger = print  # Default logger
    
    def set_logger(self, logger_func: Callable[[str], None]) -> None:
        """Define a função de log a ser usada."""
        self.logger = logger_func
    
    def authenticate_athena(self, access_key: str, secret_key: str, 
                          session_token: str, region: str = DEFAULT_REGION) -> bool:
        """
        Autentica no AWS Athena usando as credenciais fornecidas.
        
        Args:
            access_key: AWS Access Key ID
            secret_key: AWS Secret Access Key
            session_token: AWS Session Token
            region: Região AWS (padrão: sa-east-1)
            
        Returns:
            bool: True se a autenticação for bem-sucedida, False caso contrário
        """
        try:
            self.logger(f"Tentando autenticar no AWS Athena (região: {region})...")
            
            self.athena_client = boto3.client(
                'athena',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name=region
            )
            
            # Testa a autenticação com uma chamada simples
            self.athena_client.list_work_groups()
            self.logger("✅ Autenticação bem-sucedida no AWS Athena!")
            return True
            
        except ClientError as e:
            self.logger(f"❌ Falha na autenticação do Athena: {e}")
            return False
        except Exception as e:
            self.logger(f"❌ Erro inesperado durante autenticação: {e}")
            return False
    
    def validate_table_names(self, table_names: List[str]) -> List[str]:
        """
        Valida e limpa uma lista de nomes de tabelas.
        
        Args:
            table_names: Lista de nomes de tabelas
            
        Returns:
            Lista de nomes de tabelas limpos e validados
        """
        if not isinstance(table_names, list):
            self.logger("❌ Erro: Entrada deve ser uma lista de nomes de tabelas")
            return []
        
        # Limpar e validar nomes de tabelas
        clean_tables = [table.strip() for table in table_names if table and table.strip()]
        
        if not clean_tables:
            self.logger("❌ Erro: Nenhum nome de tabela válido fornecido")
            return []
        
        self.logger(f"✅ Tabelas validadas: {', '.join(clean_tables)}")
        return clean_tables
    
    def get_table_schema(self, table_name: str, database_name: str) -> Dict[str, Any]:
        """
        Obtém o schema de uma tabela do AWS Athena.
        
        Args:
            table_name: Nome da tabela
            database_name: Nome do banco de dados
            
        Returns:
            Dicionário contendo informações do schema da tabela
        """
        if not self.athena_client:
            self.logger("❌ Erro: Cliente Athena não inicializado. Execute authenticate_athena primeiro")
            return {"columns": []}
        
        self.logger(f"🔍 Verificando schema da tabela: {table_name}")
        
        try:
            # Executa query DESCRIBE na tabela
            query = f"DESCRIBE {database_name}.{table_name}"
            
            query_execution = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database_name},
                WorkGroup=self.WORKGROUP,
                ResultConfiguration={'OutputLocation': f's3://{self.S3_RESULTS_BUCKET}/'}
            )
            
            execution_id = query_execution['QueryExecutionId']
            
            # Aguarda a conclusão da consulta
            status = self._wait_for_query_completion(execution_id)
            
            if status != 'SUCCEEDED':
                self.logger(f"❌ Falha ao obter schema: Status={status}")
                return {"columns": []}
            
            # Obtém resultados
            results = self.athena_client.get_query_results(QueryExecutionId=execution_id)
            
            # Processa resultados
            columns = []
            for row in results['ResultSet']['Rows'][1:]:  # Pula o cabeçalho
                if len(row['Data']) >= 2:
                    col_name = row['Data'][0].get('VarCharValue', '')
                    col_type = row['Data'][1].get('VarCharValue', '')
                    
                    if col_name and col_type:
                        columns.append({
                            'name': col_name,
                            'type': col_type
                        })
            
            self.logger(f"✅ Schema obtido para {table_name}: {len(columns)} colunas encontradas")
            return {
                "table_name": table_name,
                "columns": columns
            }
            
        except Exception as e:
            self.logger(f"❌ Erro ao obter schema para '{table_name}': {e}")
            return {"columns": []}
    
    def _wait_for_query_completion(self, query_execution_id: str) -> str:
        """
        Aguarda a conclusão de uma consulta Athena.
        
        Args:
            query_execution_id: ID da execução da consulta
            
        Returns:
            Status final da consulta
        """
        state = 'RUNNING'
        max_retries = 50  # Evita loop infinito
        retries = 0
        
        while state in ['RUNNING', 'QUEUED'] and retries < max_retries:
            query_status = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = query_status['QueryExecution']['Status']['State']
            
            if state in ['FAILED', 'CANCELLED']:
                reason = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown reason')
                self.logger(f"❌ Consulta falhou: {reason}")
                return state
            
            if state == 'SUCCEEDED':
                break
                
            retries += 1
            time.sleep(1)  # Aguarda 1 segundo entre verificações
        
        return state
    
    def analyze_table_schemas(self, schemas: List[Dict[str, Any]]) -> Tuple[Dict[str, List[str]], Set[str]]:
        """
        Analisa os schemas das tabelas para identificar inconsistências de tipos.
        
        Args:
            schemas: Lista de schemas de tabelas
            
        Returns:
            Tupla contendo:
            - Dicionário de tipos de coluna (nome da coluna -> lista de tipos)
            - Conjunto de todas as colunas encontradas em todas as tabelas
        """
        self.logger("🔍 Analisando schemas de tabelas...")
        
        # Mapeia colunas para seus tipos em diferentes tabelas
        column_types: Dict[str, List[str]] = {}
        all_columns: Set[str] = set()
        
        for schema in schemas:
            table_name = schema.get('table_name', 'unknown')
            columns = schema.get('columns', [])
            
            for column in columns:
                col_name = column['name']
                col_type = column['type']
                
                all_columns.add(col_name)
                
                if col_name not in column_types:
                    column_types[col_name] = []
                
                if col_type not in column_types[col_name]:
                    column_types[col_name].append(col_type)
        
        # Registra tipos inconsistentes
        inconsistent_columns = []
        for col, types in column_types.items():
            if len(types) > 1:
                inconsistent_columns.append(f"{col} ({', '.join(types)})")
        
        if inconsistent_columns:
            self.logger(f"⚠️ Colunas com tipos inconsistentes: {', '.join(inconsistent_columns)}")
        else:
            self.logger("✅ Nenhuma inconsistência de tipo encontrada")
        
        return column_types, all_columns
    
    def generate_unified_query(self, 
                           table_names: List[str], 
                           database: str,
                           column_types: Dict[str, List[str]], 
                           all_columns: Set[str]) -> str:
        """
        Gera uma consulta SQL unificada otimizada para o Athena.
        
        Args:
            table_names: Lista de nomes de tabelas
            database: Nome do banco de dados
            column_types: Mapeamento de colunas para seus tipos
            all_columns: Conjunto de todas as colunas
            
        Returns:
            Query SQL unificada
        """
        self.logger("🔧 Gerando consulta unificada...")
        
        # Gera CTE para cada tabela
        cte_statements = []
        final_column_parts = []
        
        # Determina o tipo de dados ideal para cada coluna
        column_final_types = {}
        for col, types in column_types.items():
            # Determina o tipo mais apropriado para cada coluna
            if 'string' in types:
                # Se tiver string, converte tudo para string para compatibilidade
                column_final_types[col] = 'string'
            elif 'double' in types or 'float' in types:
                # Se tiver double ou float, converte números para double
                column_final_types[col] = 'double'
            elif 'integer' in types or 'int' in types or 'bigint' in types:
                # Se tiver apenas inteiros, usa bigint
                column_final_types[col] = 'bigint'
            elif 'boolean' in types:
                column_final_types[col] = 'boolean'
            elif 'date' in types or 'timestamp' in types:
                # Para datas, prefere timestamp se disponível
                column_final_types[col] = 'timestamp' if 'timestamp' in types else 'date'
            else:
                # Para outros tipos, usa o primeiro tipo encontrado
                column_final_types[col] = types[0]
        
        # Gera a parte COALESCE para a consulta final
        for col in sorted(all_columns):
            final_type = column_final_types.get(col, 'string')
            cast_parts = []
            
            # Cria partes CAST para cada tabela/CTE
            for i, table in enumerate(table_names):
                cte_name = f"cte_{i}"
                cast_parts.append(f"{cte_name}.{col}")
            
            # Usa COALESCE para pegar o primeiro valor não nulo
            coalesce_stmt = f"COALESCE({', '.join(cast_parts)})"
            
            # Se necessário, adiciona CAST para converter ao tipo final
            if final_type not in ['string', 'varchar']:
                final_column_parts.append(f"CAST({coalesce_stmt} AS {final_type}) AS {col}")
            else:
                final_column_parts.append(f"{coalesce_stmt} AS {col}")
        
        # Gera as CTEs para cada tabela
        for i, table in enumerate(table_names):
            cte_name = f"cte_{i}"
            select_parts = []
            
            # Para cada coluna possível, adiciona a coluna ou NULL
            for col in sorted(all_columns):
                select_parts.append(f"{col}")
            
            # Constrói a CTE completa
            cte_query = f"""
{cte_name} AS (
    SELECT 
        {', '.join(select_parts)}
    FROM {database}.{table}
)"""
            cte_statements.append(cte_query)
        
        # Constrói a consulta final
        joins = []
        first_cte = f"cte_0"
        
        # Cria JOINs para as tabelas restantes
        for i in range(1, len(table_names)):
            cte_name = f"cte_{i}"
            # Usando FULL OUTER JOIN para incluir todas as linhas de todas as tabelas
            joins.append(f"FULL OUTER JOIN {cte_name} ON 1=1")  # Join sem condição para fazer produto cartesiano
        
        # Constrói a consulta SQL completa
        query = f"""WITH 
{', '.join(cte_statements)}

SELECT
    {', '.join(final_column_parts)}
FROM {first_cte}
{' '.join(joins)}
"""
        
        self.logger("✅ Consulta unificada gerada com sucesso!")
        return query
    
    def execute_pipeline(self, 
                      access_key: str, 
                      secret_key: str, 
                      session_token: str,
                      region: str,
                      database: str,
                      table_names: List[str]) -> Optional[str]:
        """
        Executa o pipeline completo de unificação de consultas.
        
        Args:
            access_key: AWS Access Key ID
            secret_key: AWS Secret Access Key
            session_token: AWS Session Token
            region: Região AWS
            database: Nome do banco de dados
            table_names: Lista de nomes de tabelas
            
        Returns:
            Query SQL unificada ou None em caso de falha
        """
        self.logger("🚀 Iniciando pipeline de unificação de consultas...")
        
        # 1. Autenticação
        if not self.authenticate_athena(access_key, secret_key, session_token, region):
            self.logger("❌ Pipeline abortado devido a falha na autenticação")
            return None
        
        # 2. Validação de tabelas
        validated_tables = self.validate_table_names(table_names)
        if not validated_tables:
            self.logger("❌ Pipeline abortado devido a nomes de tabelas inválidos")
            return None
        
        # 3. Obtenção de schemas
        table_schemas = []
        for table in validated_tables:
            schema = self.get_table_schema(table, database)
            if not schema['columns']:
                self.logger(f"⚠️ Aviso: Nenhuma coluna encontrada para a tabela {table}")
            table_schemas.append(schema)
        
        # 4. Análise de schemas
        column_types, all_columns = self.analyze_table_schemas(table_schemas)
        if not all_columns:
            self.logger("❌ Pipeline abortado: Nenhuma coluna encontrada nas tabelas")
            return None
        
        # 5. Geração de query unificada
        unified_query = self.generate_unified_query(validated_tables, database, column_types, all_columns)
        
        self.logger("✅ Pipeline concluído com sucesso!")
        return unified_query


class OrionGUI:
    """Interface gráfica para o Orion SQL Query Unifier."""
    
    def __init__(self, root: tk.Tk):
        """
        Inicializa a interface gráfica.
        
        Args:
            root: Janela principal do Tkinter
        """
        self.root = root
        self.root.title("Orion SQL Query Unifier")
        self.root.geometry("900x700")
        
        self.unifier = OrionQueryUnifier()
        self.unifier.set_logger(self.log)
        
        self._create_ui()
    
    def _create_ui(self):
        """Cria os elementos da interface gráfica."""
        # Frame principal
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Cabeçalho
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(
            header_frame, 
            text="Orion SQL Query Unifier", 
            font=("Arial", 16, "bold")
        ).pack(side=tk.LEFT)
        
        # Credenciais AWS
        creds_frame = ttk.LabelFrame(main_frame, text="Credenciais AWS", padding="10")
        creds_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(creds_frame, text="Access Key:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.access_key_entry = ttk.Entry(creds_frame, width=40)
        self.access_key_entry.grid(row=0, column=1, sticky=tk.W, pady=2)
        
        ttk.Label(creds_frame, text="Secret Key:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.secret_key_entry = ttk.Entry(creds_frame, width=40, show="*")
        self.secret_key_entry.grid(row=1, column=1, sticky=tk.W, pady=2)
        
        ttk.Label(creds_frame, text="Session Token:").grid(row=2, column=0, sticky=tk.W, pady=2)
        self.session_token_entry = ttk.Entry(creds_frame, width=40)
        self.session_token_entry.grid(row=2, column=1, sticky=tk.W, pady=2)
        
        ttk.Label(creds_frame, text="Região:").grid(row=3, column=0, sticky=tk.W, pady=2)
        self.region_entry = ttk.Entry(creds_frame, width=40)
        self.region_entry.insert(0, OrionQueryUnifier.DEFAULT_REGION)
        self.region_entry.grid(row=3, column=1, sticky=tk.W, pady=2)
        
        # Configuração de banco de dados
        db_frame = ttk.LabelFrame(main_frame, text="Configuração do Banco de Dados", padding="10")
        db_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(db_frame, text="Database:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.database_entry = ttk.Entry(db_frame, width=40)
        self.database_entry.grid(row=0, column=1, sticky=tk.W, pady=2)
        
        # Tabelas
        tables_frame = ttk.LabelFrame(main_frame, text="Tabelas", padding="10")
        tables_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(tables_frame, text="Número de tabelas:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.num_tables_entry = ttk.Entry(tables_frame, width=10)
        self.num_tables_entry.grid(row=0, column=1, sticky=tk.W, pady=2)
        
        ttk.Button(
            tables_frame, 
            text="Gerar campos", 
            command=self._generate_table_fields
        ).grid(row=0, column=2, padx=(10, 0), pady=2)
        
        # Container para campos de tabela
        self.tables_container = ttk.Frame(tables_frame)
        self.tables_container.grid(row=1, column=0, columnspan=3, sticky=tk.W+tk.E, pady=5)
        
        self.table_entries = []
        
        # Área de log
        log_frame = ttk.LabelFrame(main_frame, text="Log", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        self.log_text = tk.Text(log_frame, height=10, wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Scrollbar para o log
        scrollbar = ttk.Scrollbar(self.log_text, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)
        
        # Área de resultado
        result_frame = ttk.LabelFrame(main_frame, text="Query Unificada", padding="10")
        result_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        self.result_text = tk.Text(result_frame, height=10, wrap=tk.WORD)
        self.result_text.pack(fill=tk.BOTH, expand=True)
        
        # Scrollbar para o resultado
        result_scrollbar = ttk.Scrollbar(self.result_text, command=self.result_text.yview)
        result_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.result_text.config(yscrollcommand=result_scrollbar.set)
        
        # Botões
        buttons_frame = ttk.Frame(main_frame)
        buttons_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(
            buttons_frame, 
            text="Unificar Consultas", 
            command=self._unify_queries
        ).pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(
            buttons_frame, 
            text="Copiar Query", 
            command=self._copy_query
        ).pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(
            buttons_frame, 
            text="Limpar", 
            command=self._clear_all
        ).pack(side=tk.LEFT)
        
        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Pronto")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)
    
    def _generate_table_fields(self):
        """Gera campos de entrada para as tabelas."""
        try:
            num_tables = int(self.num_tables_entry.get())
            if num_tables <= 0:
                messagebox.showerror("Erro", "O número de tabelas deve ser maior que zero")
                return
                
            # Limpa o container atual
            for widget in self.tables_container.winfo_children():
                widget.destroy()
            
            self.table_entries = []
            
            # Cria novos campos
            for i in range(num_tables):
                frame = ttk.Frame(self.tables_container)
                frame.pack(fill=tk.X, pady=2)
                
                ttk.Label(frame, text=f"Tabela {i+1}:").pack(side=tk.LEFT)
                entry = ttk.Entry(frame, width=40)
                entry.pack(side=tk.LEFT, padx=(5, 0))
                self.table_entries.append(entry)
            
            self.log(f"✅ Campos para {num_tables} tabelas gerados")
            
        except ValueError:
            messagebox.showerror("Erro", "Digite um número válido de tabelas")
    
    def _unify_queries(self):
        """Executa o pipeline de unificação de consultas."""
        # Coleta os dados da interface
        access_key = self.access_key_entry.get().strip()
        secret_key = self.secret_key_entry.get().strip()
        session_token = self.session_token_entry.get().strip()
        region = self.region_entry.get().strip() or OrionQueryUnifier.DEFAULT_REGION
        database = self.database_entry.get().strip()
        
        # Coleta nomes das tabelas
        table_names = [entry.get().strip() for entry in self.table_entries if entry.get().strip()]
        
        # Validação básica
        if not access_key or not secret_key or not session_token:
            messagebox.showerror("Erro", "As credenciais AWS são obrigatórias")
            return
        
        if not database:
            messagebox.showerror("Erro", "O nome do banco de dados é obrigatório")
            return
        
        if not table_names or len(table_names) < 2:
            messagebox.showerror("Erro", "Pelo menos duas tabelas são necessárias")
            return
        
        # Limpa a área de resultado
        self.result_text.delete(1.0, tk.END)
        
        # Atualiza status
        self.status_var.set("Processando...")
        self.root.update()
        
        # Executa o pipeline
        try:
            unified_query = self.unifier.execute_pipeline(
                access_key=access_key,
                secret_key=secret_key,
                session_token=session_token,
                region=region,
                database=database,
                table_names=table_names
            )
            
            if unified_query:
                self.result_text.delete(1.0, tk.END)
                self.result_text.insert(tk.END, unified_query)
                self.status_var.set("Consulta unificada gerada com sucesso")
                self.log("✅ Consulta unificada gerada e exibida na área de resultado")
            else:
                self.status_var.set("Falha ao gerar consulta unificada")
                
        except Exception as e:
            self.log(f"❌ Erro ao executar pipeline: {e}")
            self.status_var.set("Erro ao executar pipeline")
            messagebox.showerror("Erro", f"Falha ao executar pipeline: {e}")
    
    def _copy_query(self):
        """Copia a consulta gerada para a área de transferência."""
        query = self.result_text.get(1.0, tk.END).strip()
        if query:
            self.root.clipboard_clear()
            self.root.clipboard_append(query)
            self.status_var.set("Consulta copiada para a área de transferência")
            self.log("📋 Consulta copiada para a área de transferência")
        else:
            messagebox.showinfo("Aviso", "Não há consulta para copiar")
    
    def _clear_all(self):
        """Limpa todos os campos do formulário."""
        self.access_key_entry.delete(0, tk.END)
        self.secret_key_entry.delete(0, tk.END)
        self.session_token_entry.delete(0, tk.END)
        self.region_entry.delete(0, tk.END)
        self.region_entry.insert(0, OrionQueryUnifier.DEFAULT_REGION)
        self.database_entry.delete(0, tk.END)
        self.num_tables_entry.delete(0, tk.END)
        
        # Limpa tabelas
        for widget in self.tables_container.winfo_children():
            widget.destroy()
        self.table_entries = []
        
        # Limpa log e resultado
        self.log_text.delete(1.0, tk.END)
        self.result_text.delete(1.0, tk.END)
        
        self.status_var.set("Todos os campos limpos")
        self.log("🧹 Todos os campos foram limpos")
    
    def log(self, message: str):
        """
        Adiciona uma mensagem à área de log.
        
        Args:
            message: Mensagem a ser adicionada
        """
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)
        self.root.update()


def execute_cli_pipeline():
    """Executa o pipeline via linha de comando."""
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Orion SQL Query Unifier')
    parser.add_argument('--access-key', required=True, help='AWS Access Key')
    parser.add_argument('--secret-key', required=True, help='AWS Secret Key')
    parser.add_argument('--session-token', required=True, help='AWS Session Token')
    parser.add_argument('--region', default='sa-east-1', help='AWS Region')
    parser.add_argument('--database', required=True, help='Athena Database')
    parser.add_argument('--tables', required=True, help='Table names (comma-separated)')
    
    args = parser.parse_args()
    
    unifier = OrionQueryUnifier()
    unified_query = unifier.execute_pipeline(
        access_key=args.access_key,
        secret_key=args.secret_key,
        session_token=args.session_token,
        region=args.region,
        database=args.database,
        table_names=args.tables.split(',')
    )
    
    if unified_query:
        print("\n=== UNIFIED QUERY ===\n")
        print(unified_query)
        return 0
    else:
        print("Failed to generate unified query. See log for details.")
        return 1


def main():
    """Função principal que inicia a aplicação."""
    # Verifica se está sendo executado em modo CLI ou GUI
    import sys