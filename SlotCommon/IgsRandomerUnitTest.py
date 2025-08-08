import unittest
from unittest.mock import patch

from IgsRandomer import IgsRandomer
# Assuming this is the class you provided above

# Methods as you provided

class TestIgsRandomer(unittest.TestCase):

    def setUp(self):
        self.randomer = IgsRandomer()

    def test_guess(self):
        # 測試100萬次，誤差必須在0.1%以內
        def f(times, rate):
            win = 0
            for i in range(times):
                if self.randomer.guess(rate):
                    win += 1
            return win / times

        self.assertAlmostEqual(f(1000000, 0.05), 0.05, delta=0.001)
        self.assertAlmostEqual(f(1000000, 0.55), 0.55, delta=0.001)
        self.assertAlmostEqual(f(1000000, 0.95), 0.95, delta=0.001)


    def test_guess_new(self):
        # 測試100萬次，誤差必須在0.1%以內
        def f(times, rate):
            win = 0
            for i in range(times):
                if self.randomer.guess_new(rate):
                    win += 1
            return win / times

        self.assertAlmostEqual(f(1000000, 0.05), 0.05, delta=0.001)
        self.assertAlmostEqual(f(1000000, 0.55), 0.55, delta=0.001)
        self.assertAlmostEqual(f(1000000, 0.95), 0.95, delta=0.001)

    # def test_get_weights_index(self):
    #     with patch('random.Random.randrange', return_value=5):
    #         self.assertEqual(self.randomer.get_weights_index(weights=[1, 4, 6]), 2)
    #
    #     with patch('random.Random.randrange', return_value=1):
    #         self.assertEqual(self.randomer.get_weights_index(weights=[1, 4, 6]), 1)

    def test_get_total_weights(self):
        self.assertEqual(self.randomer.get_total_weights([1, 2, 3]), 6)
        self.assertEqual(self.randomer.get_total_weights([100]), 100)
        self.assertEqual(self.randomer.get_total_weights([]), 0)

    def test_get_total_weights_new(self):
        self.assertEqual(self.randomer.get_total_weights_new([1, 2, 3]), 6)
        self.assertEqual(self.randomer.get_total_weights_new([100]), 100)
        self.assertEqual(self.randomer.get_total_weights([]), 0)

    def test_get_count_index(self):
        counts = [3, 4, 5]
        with patch.object(self.randomer, 'get_weights_index', return_value=1):
            index = self.randomer.get_count_index(counts)
            self.assertEqual(index, 1)
            self.assertEqual(counts, [3, 3, 5])

    def test_get_result_by_gate(self):
        with patch('random.Random.randint', return_value=5):
            self.assertTrue(self.randomer.get_result_by_gate(gate=[5, 10]))
            self.assertFalse(self.randomer.get_result_by_gate(gate=[4, 10]))

    def test_get_result_by_gate_extra(self):
        with patch('random.Random.randint', return_value=5):
            self.assertTrue(self.randomer.get_result_by_gate_extra(success=5, total=10))
            self.assertFalse(self.randomer.get_result_by_gate_extra(success=4, total=10))

    def test_get_result_by_weight(self):
        awards = ['a', 'b', 'c']
        weights = [10, 20, 30]
        with patch('random.Random.randint', return_value=25):
            index, award = self.randomer.get_result_by_weight(awards, weights)
            self.assertEqual(index, 1)
            self.assertEqual(award, 'b')

        with patch('random.Random.randint', return_value=60):
            index, award = self.randomer.get_result_by_weight(awards, weights)
            self.assertEqual(index, 2)
            self.assertEqual(award, 'c')


if __name__ == '__main__':
    unittest.main()
