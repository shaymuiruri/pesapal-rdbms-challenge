"""
Custom database backend wrapper for our RDBMS
This is a simplified wrapper - Django apps will use our DB directly via a manager class
"""
import sys
import os

# Add parent directory to path to import rdbms
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from rdbms.database import Database


class DBManager:
    """Manager class to interact with our RDBMS from Django"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.db = Database(db_name="todoapp", data_dir="../data")
            cls._instance._ensure_tables()
        return cls._instance
    
    def _ensure_tables(self):
        """Create tables if they don't exist"""
        # Create todos table
        result = self.db.execute("""
            CREATE TABLE todos (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,
                completed BOOLEAN,
                created_at TEXT
            );
        """)
        # Ignore if table already exists
    
    def create_todo(self, title, description="", completed=False):
        """Create a new todo"""
        from datetime import datetime
        
        # Get next ID
        result = self.db.execute("SELECT * FROM todos;")
        if result['success'] and result.get('data'):
            max_id = max(row['id'] for row in result['data'])
            next_id = max_id + 1
        else:
            next_id = 1
        
        created_at = datetime.now().isoformat()
        
        query = f"""
            INSERT INTO todos VALUES (
                {next_id},
                '{title}',
                '{description}',
                {str(completed).upper()},
                '{created_at}'
            );
        """
        result = self.db.execute(query)
        
        if result['success']:
            return {
                'id': next_id,
                'title': title,
                'description': description,
                'completed': completed,
                'created_at': created_at
            }
        return None
    
    def get_all_todos(self):
        """Get all todos"""
        result = self.db.execute("SELECT * FROM todos;")
        if result['success']:
            return result.get('data', [])
        return []
    
    def get_todo(self, todo_id):
        """Get a specific todo"""
        result = self.db.execute(f"SELECT * FROM todos WHERE id = {todo_id};")
        if result['success'] and result.get('data'):
            return result['data'][0]
        return None
    
    def update_todo(self, todo_id, title=None, description=None, completed=None):
        """Update a todo"""
        updates = []
        if title is not None:
            updates.append(f"title = '{title}'")
        if description is not None:
            updates.append(f"description = '{description}'")
        if completed is not None:
            updates.append(f"completed = {str(completed).upper()}")
        
        if not updates:
            return False
        
        query = f"UPDATE todos SET {', '.join(updates)} WHERE id = {todo_id};"
        result = self.db.execute(query)
        return result['success']
    
    def delete_todo(self, todo_id):
        """Delete a todo"""
        result = self.db.execute(f"DELETE FROM todos WHERE id = {todo_id};")
        return result['success']
    