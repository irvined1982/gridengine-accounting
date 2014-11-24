import unittest
from gridengine_accounting import UGEAccountFile, UGEAccountEntry


class TestUGE82(unittest.TestCase):
    def test_accounting(self):
        f = open("ug82_accounting")
        for ac in UGEAccountFile(f):
            self.assertIsInstance(ac, UGEAccountEntry)
            self.assertIsInstance(ac.to_dict(), dict)
if __name__ == '__main__':
    unittest.main()