"""
Simple hash-based index implementation
"""
from typing import Dict, List, Any, Set


class Index:
    """Hash-based index for fast lookups"""
    
    def __init__(self, column_name: str):
        self.column_name = column_name
        # Maps value -> set of row IDs
        self._index: Dict[Any, Set[int]] = {}
    
    def add(self, value: Any, row_id: int):
        """Add a value to the index"""
        if value not in self._index:
            self._index[value] = set()
        self._index[value].add(row_id)
    
    def remove(self, value: Any, row_id: int):
        """Remove a value from the index"""
        if value in self._index:
            self._index[value].discard(row_id)
            if not self._index[value]:
                del self._index[value]
    
    def lookup(self, value: Any) -> Set[int]:
        """Look up row IDs for a given value"""
        return self._index.get(value, set()).copy()
    
    def clear(self):
        """Clear the index"""
        self._index.clear()
    
    def rebuild(self, rows: List[Dict[str, Any]]):
        """Rebuild the index from scratch"""
        self.clear()
        for row in rows:
            if '_rowid' in row and self.column_name in row:
                value = row[self.column_name]
                self.add(value, row['_rowid'])


class IndexManager:
    """Manages multiple indexes for a table"""
    
    def __init__(self):
        self._indexes: Dict[str, Index] = {}
    
    def create_index(self, column_name: str) -> Index:
        """Create an index on a column"""
        if column_name in self._indexes:
            raise ValueError(f"Index already exists on column: {column_name}")
        
        index = Index(column_name)
        self._indexes[column_name] = index
        return index
    
    def get_index(self, column_name: str) -> Index:
        """Get an index by column name"""
        return self._indexes.get(column_name)
    
    def has_index(self, column_name: str) -> bool:
        """Check if an index exists on a column"""
        return column_name in self._indexes
    
    def drop_index(self, column_name: str):
        """Drop an index"""
        if column_name in self._indexes:
            del self._indexes[column_name]
    
    def update_indexes(self, row: Dict[str, Any], operation: str):
        """
        Update all indexes when a row changes
        
        Args:
            row: The row being modified
            operation: 'insert', 'update', or 'delete'
        """
        if '_rowid' not in row:
            return
        
        row_id = row['_rowid']
        
        for column_name, index in self._indexes.items():
            if column_name in row:
                value = row[column_name]
                
                if operation == 'insert':
                    index.add(value, row_id)
                elif operation == 'delete':
                    index.remove(value, row_id)
                
