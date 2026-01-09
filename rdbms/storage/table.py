"""
Table storage implementation with file-based persistence
"""
import json
import os
from typing import List, Dict, Any, Optional, Callable
from .schema import TableSchema, Column


class Table:
    """Represents a database table with persistent storage"""
    
    def __init__(self, schema: TableSchema, data_dir: str = "data"):
        self.schema = schema
        self.data_dir = data_dir
        self.rows: List[Dict[str, Any]] = []
        self._next_id = 1
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Load existing data
        self._load_from_disk()
    
    @property
    def _file_path(self) -> str:
        """Get the file path for this table"""
        return os.path.join(self.data_dir, f"{self.schema.name}.json")
    
    def _load_from_disk(self):
        """Load table data from disk"""
        if os.path.exists(self._file_path):
            with open(self._file_path, 'r') as f:
                data = json.load(f)
                self.rows = data.get('rows', [])
                self._next_id = data.get('next_id', 1)
    
    def _save_to_disk(self):
        """Save table data to disk"""
        data = {
            'schema': self.schema.to_dict(),
            'rows': self.rows,
            'next_id': self._next_id
        }
        with open(self._file_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def insert(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a row into the table"""
        # Add internal row ID if not present
        if '_rowid' not in row:
            row['_rowid'] = self._next_id
            self._next_id += 1
        
        # Validate row against schema
        self.schema.validate_row(row)
        
        # Check primary key constraint
        pk_col = self.schema.get_primary_key_column()
        if pk_col:
            pk_value = row[pk_col.name]
            for existing_row in self.rows:
                if existing_row.get(pk_col.name) == pk_value:
                    raise ValueError(
                        f"Primary key violation: {pk_col.name}={pk_value} already exists"
                    )
        
        # Check unique constraints
        for col in self.schema.columns:
            if col.unique and row.get(col.name) is not None:
                for existing_row in self.rows:
                    if existing_row.get(col.name) == row[col.name]:
                        raise ValueError(
                            f"Unique constraint violation: {col.name}={row[col.name]} already exists"
                        )
        
        # Add row
        self.rows.append(row.copy())
        self._save_to_disk()
        return row
    
    def select(self, 
               columns: Optional[List[str]] = None,
               where: Optional[Callable[[Dict], bool]] = None) -> List[Dict[str, Any]]:
        """
        Select rows from the table
        
        Args:
            columns: List of column names to return (None for all)
            where: Filter function that returns True for rows to include
        """
        # Filter rows
        filtered_rows = self.rows if where is None else [
            row for row in self.rows if where(row)
        ]
        
        # Select columns
        if columns is None or '*' in columns:
            return [row.copy() for row in filtered_rows]
        else:
            result = []
            for row in filtered_rows:
                selected_row = {}
                for col in columns:
                    if col in row:
                        selected_row[col] = row[col]
                result.append(selected_row)
            return result
    
    def update(self, 
               updates: Dict[str, Any],
               where: Optional[Callable[[Dict], bool]] = None) -> int:
        """
        Update rows in the table
        
        Args:
            updates: Dictionary of column:value pairs to update
            where: Filter function for rows to update
        
        Returns:
            Number of rows updated
        """
        count = 0
        for row in self.rows:
            if where is None or where(row):
                # Apply updates
                for col_name, value in updates.items():
                    col = self.schema.get_column(col_name)
                    if col is None:
                        raise ValueError(f"Unknown column: {col_name}")
                    if not col.validate(value):
                        raise ValueError(f"Invalid value for {col_name}: {value}")
                    row[col_name] = value
                count += 1
        
        if count > 0:
            self._save_to_disk()
        
        return count
    
    def delete(self, where: Optional[Callable[[Dict], bool]] = None) -> int:
        """
        Delete rows from the table
        
        Args:
            where: Filter function for rows to delete
        
        Returns:
            Number of rows deleted
        """
        if where is None:
            count = len(self.rows)
            self.rows = []
        else:
            initial_count = len(self.rows)
            self.rows = [row for row in self.rows if not where(row)]
            count = initial_count - len(self.rows)
        
        if count > 0:
            self._save_to_disk()
        
        return count
    
    def count(self) -> int:
        """Return the number of rows in the table"""
        return len(self.rows)
    
    def drop(self):
        """Drop the table and delete its data file"""
        if os.path.exists(self._file_path):
            os.remove(self._file_path)
        self.rows = []
        