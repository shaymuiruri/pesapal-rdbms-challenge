"""
REPL (Read-Eval-Print Loop) for interactive database queries
"""
from .database import Database
from tabulate import tabulate
import sys


class REPL:
    """Interactive database shell"""
    
    def __init__(self, db_name: str = "mydb"):
        self.db = Database(db_name)
        self.running = True
    
    def start(self):
        """Start the REPL"""
        print(f"Welcome to PesapalDB REPL")
        print(f"Database: {self.db.db_name}")
        print("Type 'help' for commands, 'exit' to quit\n")
        
        while self.running:
            try:
                # Get input
                query = input("db> ").strip()
                
                # Handle special commands
                if not query:
                    continue
                
                if query.lower() == 'exit' or query.lower() == 'quit':
                    print("Goodbye!")
                    break
                
                if query.lower() == 'help':
                    self._print_help()
                    continue
                
                if query.lower() == 'tables':
                    self._list_tables()
                    continue
                
                if query.lower().startswith('describe '):
                    table_name = query.split()[1]
                    self._describe_table(table_name)
                    continue
                
                # Execute query
                result = self.db.execute(query)
                
                # Display result
                if result['success']:
                    print(f"✓ {result['message']}")
                    
                    if 'data' in result and result['data']:
                        self._print_table(result['data'])
                else:
                    print(f"✗ {result['message']}")
                
                print()  # Empty line for readability
            
            except KeyboardInterrupt:
                print("\n\nUse 'exit' to quit")
                continue
            except EOFError:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"✗ Error: {str(e)}\n")
    
    def _print_help(self):
        """Print help information"""
        help_text = """
Available Commands:
  help              - Show this help message
  tables            - List all tables
  describe <table>  - Show table schema
  exit/quit         - Exit the REPL

SQL Commands:
  CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...);
  INSERT INTO table_name VALUES (val1, val2, ...);
  INSERT INTO table_name (col1, col2) VALUES (val1, val2);
  SELECT * FROM table_name;
  SELECT col1, col2 FROM table_name WHERE condition;
  SELECT * FROM table1 JOIN table2 ON table1.col = table2.col;
  UPDATE table_name SET col = value WHERE condition;
  DELETE FROM table_name WHERE condition;

Supported Data Types:
  INTEGER, TEXT, BOOLEAN, FLOAT

Constraints:
  PRIMARY KEY, UNIQUE, NOT NULL

Example:
  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
  INSERT INTO users VALUES (1, 'Alice');
  SELECT * FROM users WHERE id = 1;
"""
        print(help_text)
    
    def _list_tables(self):
        """List all tables"""
        tables = self.db.list_tables()
        if tables:
            print("Tables:")
            for table in tables:
                print(f"  - {table}")
        else:
            print("No tables found")
        print()
    
    def _describe_table(self, table_name: str):
        """Describe table schema"""
        if table_name not in self.db.tables:
            print(f"Table '{table_name}' does not exist")
            return
        
        table = self.db.tables[table_name]
        schema = table.schema
        
        print(f"\nTable: {table_name}")
        print("Columns:")
        
        columns_info = []
        for col in schema.columns:
            constraints = []
            if col.primary_key:
                constraints.append("PRIMARY KEY")
            if col.unique:
                constraints.append("UNIQUE")
            if col.not_null:
                constraints.append("NOT NULL")
            
            columns_info.append([
                col.name,
                col.data_type.value,
                ", ".join(constraints) if constraints else "-"
            ])
        
        print(tabulate(columns_info, 
                      headers=["Column", "Type", "Constraints"],
                      tablefmt="grid"))
        print(f"\nRow count: {table.count()}\n")
    
    def _print_table(self, rows: list):
        """Print rows in a nice table format"""
        if not rows:
            print("(empty result)")
            return
        
        # Get all column names
        columns = set()
        for row in rows:
            columns.update(row.keys())
        
        # Remove internal columns
        columns = sorted([c for c in columns if not c.startswith('_')])
        
        # Build table data
        table_data = []
        for row in rows:
            table_data.append([row.get(col, 'NULL') for col in columns])
        
        print(tabulate(table_data, headers=columns, tablefmt="grid"))
        print(f"\n{len(rows)} row(s) returned")


def main():
    """Main entry point for REPL"""
    repl = REPL()
    repl.start()


if __name__ == '__main__':
    main()