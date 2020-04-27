import unittest
import learning.arithmetic

class SimpleTest(unittest.TestCase):

    # Returns True or False.
    def test_add(self):
        self.assertEqual(learning.arithmetic.add(5, 5), 25)

    def test_multiply(self):
        self.assertEqual(learning.arithmetic.multiply(5, 5), 25)

if __name__ == '__main__':
    unittest.main()
