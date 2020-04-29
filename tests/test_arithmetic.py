import unittest
from app import arithmetic


class SimpleTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_add(self):
        self.assertEqual(arithmetic.add(5, 5), 10)


if __name__ == '__main__':
    unittest.main()
~                   
