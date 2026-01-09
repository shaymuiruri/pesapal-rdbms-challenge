"""
SQL query parser using sqlparse library
"""
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, DML
from typing import Dict, List, Any, Optional
import re


class SQLParser:
    """Parse SQL queries into structured format"""
    
    @staticmethod
    def parse(query: str) -> Dict[str, Any]:
        """
        Parse a SQL query and return structured information
        
        Returns a dict with keys like:
        - type: 'CREATE', 'INSERT', 'SELECT', 'UPDATE', 'DELETE'
        - table: table name
        - columns: list of column names/definitions
        - values: values to insert/update
        - where: WHERE clause condition
        """
        # Remove extra whitespace and parse
        query = query.strip()
        if not query.endswith(';'):
            query += ';'
        
        parsed = sqlparse.parse(query)[0]
        
        # Get query type
        query_type = None
        for token in parsed.tokens:
            if token.ttype is DML or token.ttype is Keyword.DDL:
                query_type = token.value.upper()
                break
        
        if query_type == 'CREATE':
            return SQLParser._parse_create(parsed)
        elif query_type == 'INSERT':
            return SQLParser._parse_insert(parsed)
        elif query_type == 'SELECT':
            return SQLParser._parse_select(parsed)
        elif query_type == 'UPDATE':
            return SQLParser._parse_update(parsed)
        elif query_type == 'DELETE':
            return SQLParser._parse_delete(parsed)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")
    
    @staticmethod
    def _parse_create(parsed) -> Dict[str, Any]:
        """Parse CREATE TABLE statement"""
        query_str = str(parsed)
        
        # Extract table name
        match = re.search(r'CREATE\s+TABLE\s+(\w+)', query_str, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid CREATE TABLE syntax")
        
        table_name = match.group(1)
        
        # Extract column definitions
        match = re.search(r'\((.*)\)', query_str, re.DOTALL)
        if not match:
            raise ValueError("Missing column definitions")
        
        columns_str = match.group(1)
        columns = []
        
        for col_def in columns_str.split(','):
            col_def = col_def.strip()
            parts = col_def.split()
            
            if len(parts) < 2:
                continue
            
            col_name = parts[0]
            col_type = parts[1].upper()
            
            # Check for constraints
            constraints = ' '.join(parts[2:]).upper()
            
            columns.append({
                'name': col_name,
                'type': col_type,
                'primary_key': 'PRIMARY KEY' in constraints,
                'unique': 'UNIQUE' in constraints,
                'not_null': 'NOT NULL' in constraints
            })
        
        return {
            'type': 'CREATE',
            'table': table_name,
            'columns': columns
        }
    
    @staticmethod
    def _parse_insert(parsed) -> Dict[str, Any]:
        """Parse INSERT statement"""
        query_str = str(parsed)
        
        # Extract table name
        match = re.search(r'INSERT\s+INTO\s+(\w+)', query_str, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid INSERT syntax")
        
        table_name = match.group(1)
        
        # Extract columns (if specified)
        columns = None
        col_match = re.search(r'\(([^)]+)\)\s+VALUES', query_str, re.IGNORECASE)
        if col_match:
            columns = [c.strip() for c in col_match.group(1).split(',')]
        
        # Extract values
        val_match = re.search(r'VALUES\s*\(([^)]+)\)', query_str, re.IGNORECASE)
        if not val_match:
            raise ValueError("Missing VALUES clause")
        
        values_str = val_match.group(1)
        values = []
        
        for val in values_str.split(','):
            val = val.strip()
            # Remove quotes from strings
            if (val.startswith("'") and val.endswith("'")) or \
               (val.startswith('"') and val.endswith('"')):
                values.append(val[1:-1])
            elif val.upper() == 'NULL':
                values.append(None)
            elif val.upper() == 'TRUE':
                values.append(True)
            elif val.upper() == 'FALSE':
                values.append(False)
            elif '.' in val:
                values.append(float(val))
            else:
                try:
                    values.append(int(val))
                except ValueError:
                    values.append(val)
        
        return {
            'type': 'INSERT',
            'table': table_name,
            'columns': columns,
            'values': values
        }
    
    @staticmethod
    def _parse_select(parsed) -> Dict[str, Any]:
        """Parse SELECT statement"""
        query_str = str(parsed)
        
        # Extract columns
        col_match = re.search(r'SELECT\s+(.*?)\s+FROM', query_str, re.IGNORECASE | re.DOTALL)
        if not col_match:
            raise ValueError("Invalid SELECT syntax")
        
        columns_str = col_match.group(1).strip()
        if columns_str == '*':
            columns = ['*']
        else:
            columns = [c.strip() for c in columns_str.split(',')]
        
        # Extract table name
        table_match = re.search(r'FROM\s+(\w+)', query_str, re.IGNORECASE)
        if not table_match:
            raise ValueError("Missing FROM clause")
        
        table_name = table_match.group(1)
        
        # Extract WHERE clause (if present)
        where_clause = None
        where_match = re.search(r'WHERE\s+(.+?)(?:;|$)', query_str, re.IGNORECASE)
        if where_match:
            where_clause = where_match.group(1).strip()
        
        # Extract JOIN (if present)
        join_table = None
        join_condition = None
        join_match = re.search(r'(INNER\s+)?JOIN\s+(\w+)\s+ON\s+(.+?)(?:WHERE|;|$)', 
                               query_str, re.IGNORECASE)
        if join_match:
            join_table = join_match.group(2)
            join_condition = join_match.group(3).strip()
        
        return {
            'type': 'SELECT',
            'columns': columns,
            'table': table_name,
            'where': where_clause,
            'join_table': join_table,
            'join_condition': join_condition
        }
    
    @staticmethod
    def _parse_update(parsed) -> Dict[str, Any]:
        """Parse UPDATE statement"""
        query_str = str(parsed)
        
        # Extract table name
        match = re.search(r'UPDATE\s+(\w+)', query_str, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid UPDATE syntax")
        
        table_name = match.group(1)
        
        # Extract SET clause
        set_match = re.search(r'SET\s+(.+?)(?:WHERE|;|$)', query_str, re.IGNORECASE)
        if not set_match:
            raise ValueError("Missing SET clause")
        
        updates = {}
        for assignment in set_match.group(1).split(','):
            parts = assignment.split('=')
            if len(parts) != 2:
                continue
            
            col_name = parts[0].strip()
            value_str = parts[1].strip()
            
            # Parse value
            if (value_str.startswith("'") and value_str.endswith("'")) or \
               (value_str.startswith('"') and value_str.endswith('"')):
                value = value_str[1:-1]
            elif value_str.upper() == 'NULL':
                value = None
            elif value_str.upper() == 'TRUE':
                value = True
            elif value_str.upper() == 'FALSE':
                value = False
            else:
                try:
                    value = int(value_str)
                except ValueError:
                    try:
                        value = float(value_str)
                    except ValueError:
                        value = value_str
            
            updates[col_name] = value
        
        # Extract WHERE clause
        where_clause = None
        where_match = re.search(r'WHERE\s+(.+?)(?:;|$)', query_str, re.IGNORECASE)
        if where_match:
            where_clause = where_match.group(1).strip()
        
        return {
            'type': 'UPDATE',
            'table': table_name,
            'updates': updates,
            'where': where_clause
        }
    
    @staticmethod
    def _parse_delete(parsed) -> Dict[str, Any]:
        """Parse DELETE statement"""
        query_str = str(parsed)
        
        # Extract table name
        match = re.search(r'DELETE\s+FROM\s+(\w+)', query_str, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid DELETE syntax")
        
        table_name = match.group(1)
        
        # Extract WHERE clause
        where_clause = None
        where_match = re.search(r'WHERE\s+(.+?)(?:;|$)', query_str, re.IGNORECASE)
        if where_match:
            where_clause = where_match.group(1).strip()
        
        return {
            'type': 'DELETE',
            'table': table_name,
            'where': where_clause
        }
    