"""
Main database engine that coordinates all components
"""
import os
import json
from typing import Dict, List, Any, Optional
from .storage.schema import TableSchema, Column, DataType
from .storage.table import Table
from .parser.sql_parser import SQLParser
from .indexes.index import IndexManager
import re


class Database:
    """Main database engine"""
    
    def __init__(self, db_name: str = "mydb", data_dir: str = "data"):
        self.db_name = db_name
        self.data_dir = data_dir
        self.tables: Dict[str, Table] = {}
        self.indexes: Dict[str, IndexManager] = {}
        
        # Create data directory
        os.makedirs(data_dir, exist_ok=True)
        
        # Load existing tables
        self._load_database()
    
    def _load_database(self):
        """Load all existing tables from disk"""
        metadata_file = os.path.join(self.data_dir, "_metadata.json")
        
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
                
                for table_name, schema_dict in metadata.get('tables', {}).items():
                    schema = TableSchema.from_dict(schema_dict)
                    self.tables[table_name] = Table(schema, self.data_dir)
                    self.indexes[table_name] = IndexManager()
    
    def _save_metadata(self):
        """Save database metadata to disk"""
        metadata = {
            'db_name': self.db_name,
            'tables': {
                name: table.schema.to_dict() 
                for name, table in self.tables.items()
            }
        }
        
        metadata_file = os.path.join(self.data_dir, "_metadata.json")
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def execute(self, query: str) -> Dict[str, Any]:
        """
        Execute a SQL query
        
        Returns:
            Dictionary with 'success', 'message', and optional 'data' keys
        """
        try:
            parsed = SQLParser.parse(query)
            query_type = parsed['type']
            
            if query_type == 'CREATE':
                return self._execute_create(parsed)
            elif query_type == 'INSERT':
                return self._execute_insert(parsed)
            elif query_type == 'SELECT':
                return self._execute_select(parsed)
            elif query_type == 'UPDATE':
                return self._execute_update(parsed)
            elif query_type == 'DELETE':
                return self._execute_delete(parsed)
            else:
                return {
                    'success': False,
                    'message': f"Unsupported query type: {query_type}"
                }
        
        except Exception as e:
            return {
                'success': False,
                'message': f"Error: {str(e)}"
            }
    
    def _execute_create(self, parsed: Dict) -> Dict[str, Any]:
        """Execute CREATE TABLE"""
        table_name = parsed['table']
        
        if table_name in self.tables:
            return {
                'success': False,
                'message': f"Table '{table_name}' already exists"
            }
        
        # Create columns
        columns = []
        for col_def in parsed['columns']:
            data_type = DataType[col_def['type']]
            col = Column(
                name=col_def['name'],
                data_type=data_type,
                primary_key=col_def.get('primary_key', False),
                unique=col_def.get('unique', False),
                not_null=col_def.get('not_null', False)
            )
            columns.append(col)
        
        # Create schema and table
        schema = TableSchema(table_name, columns)
        self.tables[table_name] = Table(schema, self.data_dir)
        self.indexes[table_name] = IndexManager()
        
        # Save metadata
        self._save_metadata()
        
        return {
            'success': True,
            'message': f"Table '{table_name}' created successfully"
        }
    
    def _execute_insert(self, parsed: Dict) -> Dict[str, Any]:
        """Execute INSERT"""
        table_name = parsed['table']
        
        if table_name not in self.tables:
            return {
                'success': False,
                'message': f"Table '{table_name}' does not exist"
            }
        
        table = self.tables[table_name]
        
        # Build row dictionary
        if parsed['columns']:
            row = dict(zip(parsed['columns'], parsed['values']))
        else:
            # Use schema column order
            row = dict(zip(
                [col.name for col in table.schema.columns],
                parsed['values']
            ))
        
        # Insert row
        table.insert(row)
        
        return {
            'success': True,
            'message': f"1 row inserted into '{table_name}'"
        }
    
    def _execute_select(self, parsed: Dict) -> Dict[str, Any]:
        """Execute SELECT"""
        table_name = parsed['table']
        
        if table_name not in self.tables:
            return {
                'success': False,
                'message': f"Table '{table_name}' does not exist"
            }
        
        table = self.tables[table_name]
        
        # Build WHERE filter function
        where_func = None
        if parsed['where']:
            where_func = self._build_where_function(parsed['where'])
        
        # Handle JOIN
        if parsed['join_table']:
            return self._execute_join(parsed)
        
        # Execute SELECT
        columns = parsed['columns'] if parsed['columns'] != ['*'] else None
        rows = table.select(columns=columns, where=where_func)
        
        return {
            'success': True,
            'message': f"Retrieved {len(rows)} row(s)",
            'data': rows
        }
    
    def _execute_join(self, parsed: Dict) -> Dict[str, Any]:
        """Execute SELECT with JOIN"""
        table1_name = parsed['table']
        table2_name = parsed['join_table']
        
        if table1_name not in self.tables or table2_name not in self.tables:
            return {
                'success': False,
                'message': "One or both tables do not exist"
            }
        
        table1 = self.tables[table1_name]
        table2 = self.tables[table2_name]
        
        # Parse join condition (e.g., "table1.id = table2.user_id")
        join_parts = parsed['join_condition'].split('=')
        if len(join_parts) != 2:
            return {
                'success': False,
                'message': "Invalid JOIN condition"
            }
        
        left_col = join_parts[0].strip().split('.')[-1]
        right_col = join_parts[1].strip().split('.')[-1]
        
        # Perform nested loop join
        result = []
        for row1 in table1.rows:
            for row2 in table2.rows:
                if row1.get(left_col) == row2.get(right_col):
                    # Merge rows with table prefixes
                    merged = {}
                    for k, v in row1.items():
                        merged[f"{table1_name}.{k}"] = v
                    for k, v in row2.items():
                        merged[f"{table2_name}.{k}"] = v
                    result.append(merged)
        
        # Apply WHERE filter if present
        if parsed['where']:
            where_func = self._build_where_function(parsed['where'])
            result = [row for row in result if where_func(row)]
        
        return {
            'success': True,
            'message': f"Retrieved {len(result)} row(s)",
            'data': result
        }
    
    def _execute_update(self, parsed: Dict) -> Dict[str, Any]:
        """Execute UPDATE"""
        table_name = parsed['table']
        
        if table_name not in self.tables:
            return {
                'success': False,
                'message': f"Table '{table_name}' does not exist"
            }
        
        table = self.tables[table_name]
        
        # Build WHERE filter function
        where_func = None
        if parsed['where']:
            where_func = self._build_where_function(parsed['where'])
        
        # Execute UPDATE
        count = table.update(parsed['updates'], where=where_func)
        
        return {
            'success': True,
            'message': f"{count} row(s) updated"
        }
    
    def _execute_delete(self, parsed: Dict) -> Dict[str, Any]:
        """Execute DELETE"""
        table_name = parsed['table']
        
        if table_name not in self.tables:
            return {
                'success': False,
                'message': f"Table '{table_name}' does not exist"
            }
        
        table = self.tables[table_name]
        
        # Build WHERE filter function
        where_func = None
        if parsed['where']:
            where_func = self._build_where_function(parsed['where'])
        
        # Execute DELETE
        count = table.delete(where=where_func)
        
        return {
            'success': True,
            'message': f"{count} row(s) deleted"
        }
    
    def _build_where_function(self, where_clause: str):
        """Build a Python function from WHERE clause"""
        # Simple WHERE clause parser
        # Supports: column = value, column > value, column < value, etc.
        
        def where_func(row):
            # Replace column names with row access
            condition = where_clause
            
            # Handle different operators
            for op in ['>=', '<=', '!=', '=', '>', '<']:
                if op in condition:
                    parts = condition.split(op)
                    if len(parts) == 2:
                        col = parts[0].strip()
                        # Handle table.column notation
                        if '.' in col:
                            col = col.split('.')[-1]
                        
                        val = parts[1].strip()
                        
                        # Remove quotes from string values
                        if (val.startswith("'") and val.endswith("'")) or \
                           (val.startswith('"') and val.endswith('"')):
                            val = val[1:-1]
                            row_val = str(row.get(col, ''))
                        else:
                            try:
                                val = int(val)
                            except ValueError:
                                try:
                                    val = float(val)
                                except ValueError:
                                    pass
                            row_val = row.get(col)
                        
                        # Perform comparison
                        if op == '=' or op == '==':
                            return row_val == val
                        elif op == '!=':
                            return row_val != val
                        elif op == '>':
                            return row_val > val if row_val is not None else False
                        elif op == '<':
                            return row_val < val if row_val is not None else False
                        elif op == '>=':
                            return row_val >= val if row_val is not None else False
                        elif op == '<=':
                            return row_val <= val if row_val is not None else False
                    break
            
            return True
        
        return where_func
    
    def list_tables(self) -> List[str]:
        """List all tables in the database"""
        return list(self.tables.keys())
    
    def drop_table(self, table_name: str) -> Dict[str, Any]:
        """Drop a table"""
        if table_name not in self.tables:
            return {
                'success': False,
                'message': f"Table '{table_name}' does not exist"
            }
        
        self.tables[table_name].drop()
        del self.tables[table_name]
        del self.indexes[table_name]
        self._save_metadata()
        
        return {
            'success': True,
            'message': f"Table '{table_name}' dropped"
        }
    