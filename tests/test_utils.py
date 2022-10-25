from email import utils
import unittest

import sys
sys.path.append('/home/nkct/Documents/projects/python/delphi/src')
import utils

class TestUtils(unittest.TestCase):

    def test_tuple_to_sql_tuple_string(self):
        s = utils.tuple_to_sql_tuple_string(('a', 'b'))
        self.assertEqual(s, "('a', 'b')")
        self.assertIsInstance(s, str)

        s = utils.tuple_to_sql_tuple_string(('a',))
        self.assertEqual(s, "('a')")

        s = utils.tuple_to_sql_tuple_string(())
        self.assertEqual(s, "()")

    def test_sql_tuple_string_to_tuple(self):
        tup = utils.sql_tuple_string_to_tuple("('a', 'b')")
        self.assertEqual(tup, ('a', 'b'))
        self.assertIsInstance(tup, tuple)

        tup = utils.sql_tuple_string_to_tuple("('a')")
        self.assertEqual(tup, ('a',))

        tup = utils.sql_tuple_string_to_tuple("()")
        self.assertEqual(tup, ())

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
        self.assertEqual(utils.alias_properties(props), ["currentPrice", "symbol", "marketCap"])


if __name__ == '__main__':
    unittest.main()