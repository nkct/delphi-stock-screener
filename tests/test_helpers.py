import unittest
import os
import sqlite3 as db
import logging as log

import sys
sys.path.append('/home/nkct/Documents/projects/python/delphi/')

import src.helpers as helpers

class TestHelpers(unittest.TestCase):
    testing_level = "WARNING"
    
    @classmethod
    def setUpClass(cls):
        log.basicConfig(level = cls.testing_level)

    def test_clear(self):
        test_db = "test.db"
        test_table = "test"

        conn = db.connect(test_db)
        cur = conn.cursor()

        with self.subTest(i=1):
            cur.execute(f"CREATE TABLE {test_table} ( test TEXT )")

            helpers.clear(test_db, test_table)

            self.assertTrue(
                cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{test_table}';")
            )

        with self.subTest(i=2):
            cur.execute(f"CREATE TABLE {test_table} ( test TEXT )")

            with self.assertRaises(db.OperationalError) as e:
                helpers.clear(test_db, "not_test")

            self.assertEqual(
                str(e.exception),
                "no such table: not_test"
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
    if len(sys.argv) > 1:
        TestHelpers.testing_level = sys.argv.pop()
    unittest.main()