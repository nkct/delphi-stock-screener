import unittest

import sqlite3 as db
import os

import sys
sys.path.append('/home/nkct/Documents/projects/python/delphi/src')
import utils

from src.filter import filter as delphi_filter

class TestSort(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # VERY BAD PRECTICE! STRICTLY FOR TESTING! CHANGE BACK AFTER USE!
        utils.DATABASE = "test.db"
        utils.TABLE = "test"

        conn = db.connect(utils.DATABASE)
        cur = conn.cursor()

        cur.execute(f"""
                        CREATE TABLE {utils.TABLE} (
                            symbol TEXT,
                            int1 TEXT,
                            int2 TEXT,
                            str TEXT
                        );
                    """)

        cur.execute(f"""
                        INSERT INTO {utils.TABLE}
                        VALUES
                        ('SYM', '100', '500', 'abcde'),
                        ('BOL', '10', '80', 'fghij'),
                        ('TIC', '200', '731', 'qwert'),
                        ('KER', '3000', '3', 'asdfg');
                    """)
        conn.commit()

    def test_filter(self):
        self.assertEqual(
            delphi_filter(["SYM", "BOL", "TIC", "KER"], ["int1 < 200"]),
            ["SYM", "BOL"]
        )

    @classmethod
    def tearDownClass(cls):
        utils.DATABASE = "database.db"

        os.remove("test.db")

if __name__ == '__main__':
    unittest.main()