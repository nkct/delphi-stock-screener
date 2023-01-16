import unittest
import logging as log

import sys
sys.path.append('/home/nkct/Documents/projects/python/delphi/')
import src.utils as utils

class TestUtils(unittest.TestCase):
    testing_level = "WARNING"
    
    @classmethod
    def setUpClass(cls):
        log.basicConfig(level = cls.testing_level)

    def test_tuple_to_sql_tuple_string(self):
        self.assertEqual(
            utils.tuple_to_sql_tuple_string(('a', 'b')), 
            "('a', 'b')"
        )
        self.assertEqual(
            utils.tuple_to_sql_tuple_string(('a',)), 
            "('a')"
        )
        self.assertEqual(
            utils.tuple_to_sql_tuple_string(()), 
            "()"
        )

    def test_sql_tuple_string_to_tuple(self):
        self.assertEqual(
            utils.sql_tuple_string_to_tuple("('a', 'b')"), 
            ('a', 'b')
        )
        self.assertEqual(
            utils.sql_tuple_string_to_tuple("('a')"), 
            ('a',)
        )
        self.assertEqual(
            utils.sql_tuple_string_to_tuple("()"), 
            ()
        )

    def test_eval(self):
        self.assertTrue(utils.eval(12, ">", 3))
        self.assertTrue(utils.eval(12, ">", -32))
        self.assertTrue(utils.eval("c", ">", "a"))

        self.assertTrue(utils.eval(5, "<", 27))
        self.assertTrue(utils.eval(-82, "<", 31))
        self.assertTrue(utils.eval("d", "<", "k"))

        self.assertTrue(utils.eval(9, ">=", 6))
        self.assertTrue(utils.eval(43, ">=", 43))
        self.assertTrue(utils.eval(6, ">=", -74))
        self.assertTrue(utils.eval(-3, ">=", -3))
        self.assertTrue(utils.eval("l", ">=", "b"))
        self.assertTrue(utils.eval("z", ">=", "z"))

        self.assertTrue(utils.eval(1, "<=", 10))
        self.assertTrue(utils.eval(2, "<=", 2))
        self.assertTrue(utils.eval(-27, "<=", 12))
        self.assertTrue(utils.eval(-10, "<=", -10))
        self.assertTrue(utils.eval("h", "<=", "m"))
        self.assertTrue(utils.eval("p", "<=", "p"))

        self.assertTrue(utils.eval(17, "==", 17))
        self.assertTrue(utils.eval(-19, "==", -19))
        self.assertTrue(utils.eval("j", "==", "j"))

        self.assertTrue(utils.eval(56, "!=", 8))
        self.assertTrue(utils.eval(-3, "!=", -14))
        self.assertTrue(utils.eval("g", "!=", "y"))

    def test_load_aliases(self):
        aliases = utils.load_aliases()

        for key in aliases.keys():
            self.assertIsInstance(key, tuple)

    def test_alias(self):
        pass

    def test_alias_properties(self):
        props = ["Price", "TICKER", "MarketCap"]
        self.assertEqual(
            utils.alias_properties(props), 
            ["currentPrice", "symbol", "marketCap"]
        )

    def test_flaten_floats(self):
        floats = [13.5, 1.0, 7423.71, 213.0]
        self.assertEqual(
            utils.flaten_floats(floats),
            [13.5, 1, 7423.71, 213]
        )


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TestUtils.testing_level = sys.argv.pop()
    unittest.main()