#!/usr/bin/env python
"""
Script to run the PesapalDB REPL
"""
import sys
sys.path.insert(0, '.')

from rdbms.repl import main

if __name__ == '__main__':
    main()