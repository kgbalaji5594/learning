import unittest
import arithmetic

class SimpleTest(unittest.TestCase):

    # Returns True or False.
    def test_add(self):
        self.assertEqual(arithmetic.add(5, 5), 10)


if __name__ == '__main__':
    unittest.main()
