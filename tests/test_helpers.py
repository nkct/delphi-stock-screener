import unittest
import os
import sqlite3 as db
import logging as log

import sys
sys.path.append('/home/nkct/Documents/projects/python/delphi/')

import src.helpers as helpers

class TestHelpers(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        log.basicConfig(level=log.DEBUG)

    def test_clear(self):
        test_db = "test.db"
        test_table = "test"

        conn = db.connect(test_db)
        cur = conn.cursor()

        cur.execute(f"CREATE TABLE {test_table} ( test TEXT )")

        helpers.clear(test_db, test_table)

        self.assertTrue(
            cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{test_table}';")
        )

        os.remove(test_db)

    def test_available_properties(self):
        pass

    def test_indices(self):
        pass

    def test_new_index(self):
        pass

    def test_delete_index(self):
        pass

if __name__ == '__main__':
    unittest.main()