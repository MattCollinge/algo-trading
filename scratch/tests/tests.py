import unittest


# class TestCalculations(unittest.TestCase):

    # def test_swing_finder(self):
    #     calculation = Calculations(8, 2)
    #     self.assertEqual(calculation.check_swing(), 10, 'The sum is wrong.')

def nand_and_or(a, b, c):
    return not (a and b and c) and (a or b or c)


class TestLogicAssumptions(unittest.TestCase):
    def test_nand_and_or(self):
        
        a = True
        b = True
        c = True

        self.assertEqual(nand_and_or(a, b, c), False, 'The logic is wrong')

        a = True
        b = True
        c = False

        self.assertEqual(nand_and_or(a, b, c), True, 'The logic is wrong')

        a = True
        b = False
        c = False

        self.assertEqual(nand_and_or(a, b, c), True, 'The logic is wrong')

        a = False
        b = False
        c = False

        self.assertEqual(nand_and_or(a, b, c), False, 'The logic is wrong')

        a = False
        b = True
        c = True

        self.assertEqual(nand_and_or(a, b, c), True, 'The logic is wrong')

        a = False
        b = True
        c = False

        self.assertEqual(nand_and_or(a, b, c), True, 'The logic is wrong')

        a = False
        b = False
        c = True

        self.assertEqual(nand_and_or(a, b, c), True, 'The logic is wrong')


if __name__ == '__main__':
    unittest.main()