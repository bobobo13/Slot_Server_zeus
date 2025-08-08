import random


class IgsRandomer:
    RANDINT_MAX = 1000000  # 1 million

    def __init__(self):
        self._random = random.Random()

    def randint(self, a, b):
        """
        Return random integer in range [a, b], including both end points.
        using python's random.randint function.

        Parameters:
            a (int): Lower bound of the random integer.
            b (int): Upper bound of the random integer.
            a <= b <= IgsRandomer.RANDINT_MAX

        Returns:
            int: A random integer in the range [a, b].
        """
        # Validate the range of random integers
        if a > b:
            raise ValueError("a must be less than or equal to b")
        if b > IgsRandomer.RANDINT_MAX:
            raise ValueError("b must be less than or equal to {}".format(IgsRandomer.RANDINT_MAX))
        return self._random.randint(a, b)

    def choice(self, seq):
        """
        Choose a random element from a non-empty sequence.
        using python's random.choice function.

        Parameters:
            seq (sequence): A non-empty sequence.
        """
        return self._random.choice(seq)

    def sample(self, population, k, *, counts=None):
        """
        Chooses k unique random elements from a population sequence.
        Using python's random.sample function.

        Parameters:
            population (sequence): A non-empty sequence.
            k (int): The number of unique random elements to choose.
            counts (sequence): A sequence of counts for each element in the population.
        """
        return self._random.sample(population, k, counts=counts)

    def shuffle(self, x):
        """
        Shuffle list x in place, and return itself.
        using python's random.shuffle function.

        Parameters:
            x (list): A list of items to shuffle.
        """
        self._random.shuffle(x)
        return x

    def get_result_by_gate(self, gate):
        """
        Determines the result based on a gate condition.

        Parameters:
            gate (tuple): A tuple where gate[0] is the success threshold and gate[1]
                          is the maximum possible value for random comparison.
                          1 <= gate[1] <= IgsRandomer.RANDINT_MAX
        Returns:
            bool: True if the success threshold is greater than or equal to a random
                  number between 1 and gate[1], otherwise False.
        """
        # Validate the gate parameters
        if gate[1] < 1:
            raise ValueError("gate[1] must be greater than or equal to 1")
        if gate[1] > IgsRandomer.RANDINT_MAX:
            raise ValueError("gate[1] must be less than or equal to {}".format(IgsRandomer.RANDINT_MAX))
        if gate[0] <= 0:
            return False
        if gate[0] >= gate[1]:
            return True
        # Return True if the random number is within the success threshold
        return gate[0] >= self.randint(1, gate[1])

    def get_result_by_weight(self, award_list, weight_list):
        """
        Selects an award based on weighted probabilities.

        Parameters:
            award_list (list): A list of awards to choose from.
                                The length of award_list must be greater than 0.
            weight_list (list): A list of corresponding weights for each award.
                                The length of weight_list must be equal to the length of award_list.
                                The weights determine the probability of selecting each award.
                                Every element in weight_list must be greater than or equal to 0.
                                The sum of all weights must be greater than 0.

        Returns:
            tuple: A tuple containing the index of the selected award and the award itself.
                   The award is selected randomly based on the weights.
        """

        # Validate the award and weight lists
        if len(award_list) == 0:
            raise ValueError("award_list must not be empty")
        if len(award_list) != len(weight_list):
            raise ValueError("award_list and weight_list must have the same length")

        # If there is only one item in the list, return it
        if len(award_list) == 1:
            return 0, award_list[0]

        # Calculate the total weight
        total_weight = 0
        for weight in weight_list:
            if weight < 0:
                raise ValueError("every element in weight_list must be greater than or equal to 0")
            total_weight += weight
        if total_weight <= 0:
            raise ValueError("The sum of all weights must be greater than 0")

        # Variables to store the selected award and its index
        award = None
        award_index = -1
        rand_num = self.randint(1, total_weight)  # Generate a random number in the range of total weights

        # Iterate through the weights to find the corresponding award
        for index, weight in enumerate(weight_list):
            rand_num -= weight  # Subtract the current weight from the random number
            if rand_num <= 0:  # When rand_num reaches 0 or below, this award is selected
                award = award_list[index]
                award_index = index
                break  # Stop the loop since the award has been selected

        # Return the index and the selected award
        return award_index, award
