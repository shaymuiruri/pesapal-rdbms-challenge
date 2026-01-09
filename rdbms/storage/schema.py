"""
Schema definitions for tables and columns
"""
from enum import Enum
from typing import Any, List, Optional
from dataclasses import dataclass


class DataType(Enum):
    """Supported column data types"""
    INTEGER = "INTEGER"
    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"
    FLOAT = "FLOAT"


@dataclass
class Column:
    """Represents a table column"""
    name: str
    data_type: DataType
    primary_key: bool = False
    unique: bool = False
    not_null: bool = False
    
    def validate(self, value: Any) -> bool:
        """Validate if value matches column type"""
        if value is None:
            return not self.not_null and not self.primary_key
        
        if self.data_type == DataType.INTEGER:
            return isinstance(value, int)
        elif self.data_type == DataType.TEXT:
            return isinstance(value, str)
        elif self.data_type == DataType.BOOLEAN:
            return isinstance(value, bool)
        elif self.data_type == DataType.FLOAT:
            return isinstance(value, (int, float))
        
        return False
    
    def to_dict(self):
        """Convert column to dictionary for serialization"""
        return {
            'name': self.name,
            'data_type': self.data_type.value,
            'primary_key': self.primary_key,
            'unique': self.unique,
            'not_null': self.not_null
        }
    
    @staticmethod
    def from_dict(data: dict):
        """Create column from dictionary"""
        return Column(
            name=data['name'],
            data_type=DataType(data['data_type']),
            primary_key=data['primary_key'],
            unique=data['unique'],
            not_null=data['not_null']
        )


class TableSchema:
    """Represents a table schema"""
    
    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = columns
        self._validate_schema()
    
    def _validate_schema(self):
        """Validate schema constraints"""
        # Check for duplicate column names
        column_names = [col.name for col in self.columns]
        if len(column_names) != len(set(column_names)):
            raise ValueError("Duplicate column names in schema")
        
        # Check that there's at most one primary key
        pk_count = sum(1 for col in self.columns if col.primary_key)
        if pk_count > 1:
            raise ValueError("Table can have at most one primary key")
    
    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name"""
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None
    
    def get_primary_key_column(self) -> Optional[Column]:
        """Get the primary key column if exists"""
        for col in self.columns:
            if col.primary_key:
                return col
        return None
    
    def validate_row(self, row: dict) -> bool:
        """Validate a row against the schema"""
        # Check all columns are present
        for col in self.columns:
            if col.name not in row:
                if col.not_null or col.primary_key:
                    raise ValueError(f"Column '{col.name}' cannot be NULL")
                row[col.name] = None
        
        # Validate each value
        for col in self.columns:
            if not col.validate(row[col.name]):
                raise ValueError(
                    f"Invalid value for column '{col.name}': {row[col.name]}"
                )
        
        return True
    
    def to_dict(self):
        """Convert schema to dictionary for serialization"""
        return {
            'name': self.name,
            'columns': [col.to_dict() for col in self.columns]
        }
    
    @staticmethod
    def from_dict(data: dict):
        """Create schema from dictionary"""
        columns = [Column.from_dict(col) for col in data['columns']]
        return TableSchema(name=data['name'], columns=columns)
    
    