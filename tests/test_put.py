import unittest
import sqlite3 as db
import pandas as pd
import os
import json
import logging as log

import sys
sys.path.append('/home/nkct/Documents/projects/python/delphi/')
import src.utils as utils

from src.put import put

class TestPut(unittest.TestCase):
    testing_level = "WARNING"

    @classmethod
    def setUpClass(cls):
        log.basicConfig(level=cls.testing_level)
        
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



    def setUp(self):
        database = utils.get_database()
        self.table = utils.get_table()

        conn = db.connect(database)
        self.cur = conn.cursor()

        self.cur.execute(f"""
                                CREATE TABLE {self.table} (
                                    symbol TEXT
                                )
                            """)


    def test_put_insert_one(self):
        df = pd.DataFrame({
            "symbol": ["SYM"], 
            "int1": [975], 
            "int2": [62], 
            "str": ["hgli"]
        })
        put(df, "test.db", "test")

        self.cur.execute(f"SELECT * FROM {self.table}")

        self.assertEqual(
            self.cur.fetchall(),
            [
                ('SYM', 975, 62, 'hgli')
            ]
        )

    def test_put_insert_many(self):
        df = pd.DataFrame({
            "symbol": ["SYM", "BOL", "TIC", "KER"], 
            "int1": [975, 3, 12, 9684], 
            "int2": [62, 87, 432, 5687], 
            "str": ["hgli", "gejh", "olcx", "mgjn"]
        })
        put(df, "test.db", "test")

        self.cur.execute(f"SELECT * FROM {self.table}")

        self.assertEqual(
            self.cur.fetchall(),
            [
                ('SYM', 975, 62, 'hgli'), 
                ('BOL', 3, 87, 'gejh'), 
                ('TIC', 12, 432, 'olcx'), 
                ('KER', 9684, 5687, 'mgjn')
            ]
        )

    def test_put_update(self):
        df = pd.DataFrame({
            "symbol": ["SYM", "BOL", "TIC", "KER"], 
            "int1": [975, 3, 12, 9684], 
            "int2": [62, 87, 432, 5687], 
            "str": ["hgli", "gejh", "olcx", "mgjn"]
        })
        put(df, "test.db", "test")

        df = pd.DataFrame({
            "symbol": ["SYM", "BOL", "TIC", "KER"], 
            "int1": [9876, 45, 9, 53], 
            "int2": [0, 38, 43879, 87], 
            "str": ["gyujs", "kj", "rt", "k"]
        })
        put(df, "test.db", "test")

        self.cur.execute(f"SELECT * FROM {self.table}")

        self.assertEqual(
            self.cur.fetchall(),
            [
                ('SYM', 9876, 0, 'gyujs'), 
                ('BOL', 45, 38, 'kj'), 
                ('TIC', 9, 43879, 'rt'), 
                ('KER', 53, 87, 'k')
            ]
        )

    def test_put_alter(self):
        df = pd.DataFrame({
            "symbol": ["SYM", "BOL", "TIC", "KER"], 
            "int1": [975, 3, 12, 9684], 
            "int2": [62, 87, 432, 5687], 
            "str": ["hgli", "gejh", "olcx", "mgjn"]
        })
        put(df, "test.db", "test")

        df = pd.DataFrame({
            "symbol": ["SYM", "BOL", "TIC", "KER"], 
            "int1": [9876, 45, 9, 53], 
            "int2": [0, 38, 43879, 87], 
            "str": ["gyujs", "kj", "rt", "k"],
            "new_column": ["hjf", "jret", "iurt", "ihyk"]
        })
        put(df, "test.db", "test")

        self.cur.execute(f"SELECT * FROM {self.table}")

        self.assertEqual(
            self.cur.fetchall(),
            [
                ('SYM', 9876, 0, 'gyujs', 'hjf'), 
                ('BOL', 45, 38, 'kj', 'jret'), 
                ('TIC', 9, 43879, 'rt', 'iurt'), 
                ('KER', 53, 87, 'k', 'ihyk')
            ]
        )

    def test_put_create_table(self):
        self.cur.execute(f"DROP TABLE {self.table}")

        df = pd.DataFrame({
            "symbol": ["SYM", "BOL", "TIC", "KER"], 
            "int1": [975, 3, 12, 9684], 
            "int2": [62, 87, 432, 5687], 
            "str": ["hgli", "gejh", "olcx", "mgjn"]
        })
        put(df, "test.db", "test")

        self.cur.execute(f"SELECT * FROM {self.table}")

        self.assertEqual(
            self.cur.fetchall(),
            [
                ('SYM', 975, 62, 'hgli'), 
                ('BOL', 3, 87, 'gejh'), 
                ('TIC', 12, 432, 'olcx'), 
                ('KER', 9684, 5687, 'mgjn')
            ]
        )

    def test_put_incorrect_input(self):
        df = pd.DataFrame({
            "symbol": ["SYM", 17, "TIC", " "], 
            "int1": [975, 3, "kgl", 9684], 
            "int2": [62, 87, 432, None], 
            "str": [None, "gejh", "olcx", "mgjn"]
        })
        put(df, "test.db", "test")

        self.cur.execute(f"SELECT * FROM {self.table}")

        self.assertEqual(
            self.cur.fetchall(),
            [
                ('SYM', '975', '62', 'Null'), 
                ('17', '3', '87', 'gejh'), 
                ('TIC', 'kgl', '432', 'olcx'), 
                (' ', '9684', 'Null', 'mgjn')
            ]
        )

    def test_put_update_column(self):
        df = pd.DataFrame({
            "symbol": ["SYM", "BOL", "TIC", "KER"], 
            "int1": [975, 3, 12, 9684], 
            "int2": [62, 87, 432, 5687], 
            "str": ["hgli", "gejh", "olcx", "mgjn"]
        })
        put(df, "test.db", "test")

        df = pd.DataFrame({
            "symbol": ["SYM", "BOL", "TIC", "KER"], 
            "str": ["esr", "iojp", "nbgfe", "kopf"]
        })
        put(df, "test.db", "test")

        self.cur.execute(f"SELECT * FROM {self.table}")

        self.assertEqual(
            self.cur.fetchall(),
            [
                ('SYM', 975, 62, 'esr'), 
                ('BOL', 3, 87, 'iojp'), 
                ('TIC', 12, 432, 'nbgfe'), 
                ('KER', 9684, 5687, 'kopf')
            ]
        )

    def test_put_insert_column(self):
        df = pd.DataFrame({
            "symbol": ["SYM", "BOL", "TIC", "KER"], 
            "int1": [975, 3, 12, 9684], 
            "int2": [62, 87, 432, 5687], 
            "str": ["hgli", "gejh", "olcx", "mgjn"]
        })
        put(df, "test.db", "test")

        df = pd.DataFrame({
            "symbol": ["SYM", "BOL", "TIC", "KER"], 
            "str2": ["esr", "iojp", "nbgfe", "kopf"]
        })
        put(df, "test.db", "test")

        self.cur.execute(f"SELECT * FROM {self.table}")

        self.assertEqual(
            self.cur.fetchall(),
            [
                ('SYM', 975, 62, 'hgli', 'esr'), 
                ('BOL', 3, 87, 'gejh', 'iojp'), 
                ('TIC', 12, 432, 'olcx', 'nbgfe'), 
                ('KER', 9684, 5687, 'mgjn', 'kopf')
            ]
        )

    def tearDown(self):
        self.cur.execute(f"DROP TABLE {self.table}")

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
    if len(sys.argv) > 1:
        TestPut.testing_level = sys.argv.pop()
    unittest.main()