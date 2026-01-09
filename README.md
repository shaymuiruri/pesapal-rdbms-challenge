# PesapalDB - Custom RDBMS for Pesapal Junior Dev Challenge 2026

A simple relational database management system (RDBMS) built from scratch in Python, with SQL-like query support and a Django web application demo.

## 🎯 Project Overview

This project implements a functional RDBMS with the following features:
- **Table Management**: CREATE, DROP tables with schema validation
- **CRUD Operations**: INSERT, SELECT, UPDATE, DELETE
- **Data Types**: INTEGER, TEXT, BOOLEAN, FLOAT
- **Constraints**: PRIMARY KEY, UNIQUE, NOT NULL
- **Indexing**: Hash-based indexes for fast lookups
- **Joins**: INNER JOIN support
- **SQL Parser**: Parse and execute SQL-like queries
- **REPL**: Interactive command-line interface
- **Persistence**: File-based storage (JSON)
- **Demo App**: Django todo application demonstrating CRUD operations

## 🏗️ Architecture

```
├── rdbms/                  # Core RDBMS implementation
│   ├── storage/           # Storage layer
│   │   ├── schema.py      # Table schema definitions
│   │   └── table.py       # Table storage and operations
│   ├── parser/            # SQL parsing
│   │   └── sql_parser.py  # Query parser
│   ├── indexes/           # Indexing system
│   │   └── index.py       # Hash-based indexes
│   ├── database.py        # Main database engine
│   └── repl.py            # Interactive shell
├── demo_app/              # Django demo application
│   ├── todos/             # Todo app
│   │   ├── db_backend.py  # RDBMS integration
│   │   ├── views.py       # API endpoints
│   │   └── templates/     # HTML templates
│   └── todoapp/           # Django project settings
└── data/                  # Database files (created at runtime)
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- pip

### Installation

1. Clone the repository:
```bash
git clone https://github.com/shaymuiruri/pesapal-rdbms-challenge 
cd pesapal-rdbms-challenge
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the REPL

```bash
python run_repl.py
```

Try these sample commands:
```sql
-- Create a table
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);

-- Insert data
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');

-- Query data
SELECT * FROM users;
SELECT * FROM users WHERE id = 1;

-- Update data
UPDATE users SET name = 'Alice Smith' WHERE id = 1;

-- Delete data
DELETE FROM users WHERE id = 2;

-- Create related tables and join
CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
INSERT INTO posts VALUES (1, 1, 'Hello World');
SELECT * FROM users JOIN posts ON users.id = posts.user_id;
```

### Running the Demo Web App

1. Navigate to demo app directory:
```bash
cd demo_app
```

2. Run Django development server:
```bash
python manage.py runserver
```

3. Open browser to `http://127.0.0.1:8000/`

## 📚 Supported SQL Syntax

### CREATE TABLE
```sql
CREATE TABLE table_name (
    column1 TYPE [PRIMARY KEY] [UNIQUE] [NOT NULL],
    column2 TYPE,
    ...
);
```

### INSERT
```sql
INSERT INTO table_name VALUES (value1, value2, ...);
INSERT INTO table_name (column1, column2) VALUES (value1, value2);
```

### SELECT
```sql
SELECT * FROM table_name;
SELECT column1, column2 FROM table_name;
SELECT * FROM table_name WHERE condition;
SELECT * FROM table1 JOIN table2 ON table1.col = table2.col;
```

### UPDATE
```sql
UPDATE table_name SET column = value WHERE condition;
```

### DELETE
```sql
DELETE FROM table_name WHERE condition;
```

## 🔧 Implementation Details

### Storage Layer
- **File Format**: JSON-based storage for simplicity and readability
- **Schema Validation**: Type checking and constraint enforcement
- **Transaction Safety**: Atomic writes with file-based persistence

### Query Processing
1. **Parsing**: SQL queries parsed using `sqlparse` library and regex
2. **Validation**: Schema and constraint validation
3. **Execution**: Direct table operations or join algorithms
4. **Result Formatting**: Structured output for REPL and API

### Indexing
- **Hash-based indexes**: O(1) lookup for equality comparisons
- **Automatic maintenance**: Indexes updated on INSERT/UPDATE/DELETE
- **Primary key optimization**: Automatic indexing on PRIMARY KEY columns

### Join Implementation
- **Algorithm**: Nested loop join (simple but functional)
- **Condition Parsing**: Extracts column relationships from ON clause
- **Result Merging**: Combines rows with table prefixes

## 🎨 Demo Application Features

The Todo app demonstrates:
- ✅ **Create**: Add new todos with title and description
- ✅ **Read**: Display all todos with real-time updates
- ✅ **Update**: Mark todos as complete/incomplete
- ✅ **Delete**: Remove todos with confirmation
- ✅ **Statistics**: Show total, active, and completed counts
- ✅ **Responsive UI**: Modern gradient design with smooth interactions

## 🧪 Testing

Run sample queries to test functionality:

```bash
# Test REPL
python run_repl.py

# Inside REPL, try:
help
tables
CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);
INSERT INTO test VALUES (1, 'hello');
SELECT * FROM test;
describe test
```

## 📝 Design Decisions

1. **JSON Storage**: Chose JSON over binary format for:
   - Easy debugging and inspection
   - Human-readable data files
   - Simple serialization/deserialization

2. **sqlparse Library**: Used for robust SQL parsing instead of building from scratch:
   - Handles edge cases and SQL variations
   - Well-tested and maintained
   - Allows focus on core DB functionality

3. **Hash Indexes**: Implemented simple hash-based indexes:
   - O(1) lookup performance
   - Easy to understand and maintain
   - Sufficient for demo purposes

4. **Django Integration**: Created custom DB manager instead of full Django backend:
   - Simpler implementation
   - Direct control over queries
   - Clear demonstration of CRUD operations

## 🎓 Learning Outcomes

This project demonstrates understanding of:
- Database fundamentals (schemas, constraints, indexes)
- Query parsing and execution
- File I/O and data persistence
- RESTful API design
- Full-stack web development
- Software architecture and modular design


## 📦 Dependencies

- **Django** (4.2+): Web framework for demo app
- **sqlparse** (0.4.4+): SQL query parsing
- **tabulate** (0.9.0+): Pretty-print tables in REPL

## 👤 Author 

Built for the Pesapal Junior Dev Challenge 2026 by Mitchelle Muiruri

## 📄 License

This project is open source and available for educational purposes.

## 🙏 Acknowledgments

- Inspired by SQLite's design principles
- Uses sqlparse library for SQL parsing
- Django framework for web app demo


