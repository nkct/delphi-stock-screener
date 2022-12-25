import unittest
import sqlite3 as db
import os
import json

import sys
sys.path.append('/home/nkct/Documents/projects/python/delphi/src')
import utils

from src.sort import sort

class TestSort(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # STRICTLY FOR TESTING! CHANGE BACK AFTER USE!
        f = open("settings.json")
        settings = json.loads(f.read())
        f.close()

        cls.old_database = settings["database"]["path"]
        cls.old_table = settings["database"]["table"]

        settings["database"]["path"] = "test.db"
        settings["database"]["table"] = "test"

        with open("settings.json", "w") as f:
            json.dump(settings, f)

        database = utils.get_database()
        table = utils.get_table()
        

        conn = db.connect(database)
        cur = conn.cursor()

        cur.execute(f"""
                        CREATE TABLE {table} (
                            symbol TEXT,
                            int1 TEXT,
                            int2 TEXT,
                            str TEXT
                        );
                    """)

        cur.execute(f"""
                        INSERT INTO {table}
                        VALUES
                        ('SYM', '100', '500', 'abcde'),
                        ('BOL', '10', '10', 'fghij'),
                        ('TIC', '200', '731', 'qwert'),
                        ('KER', '3000', '3', 'asdfg');
                    """)
        conn.commit()

    def test_sort(self):
        self.assertEqual(
            sort(["SYM", "BOL", "TIC", "KER"], {"int1"}, True),
            ["BOL", "SYM", "TIC", "KER"]
        )
        self.assertEqual(
            sort(["SYM", "BOL", "TIC", "KER"], {"int1"}, False),
            ["KER", "TIC", "SYM", "BOL"]
        )
        # doesnt pass because numbers are stored as text
        self.assertEqual(
            sort(["SYM", "BOL", "TIC", "KER"], {"int2"}, False),
            ["TIC", "SYM", "BOL", "KER"]
        )
        self.assertEqual(
            sort(["SYM", "BOL", "TIC", "KER"], {"str"}, False),
            ["TIC", "BOL", "KER", "SYM"]
        )

    @classmethod
    def tearDownClass(cls):
        f = open("settings.json")
        settings = json.loads(f.read())
        f.close()

        settings["database"]["path"] = cls.old_database
        settings["database"]["table"] = cls.old_table

        with open("settings.json", "w") as f:
            json.dump(settings, f)

        os.remove("test.db")

if __name__ == '__main__':
    unittest.main()