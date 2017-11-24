'''Find out the minimum number of weights needed to weigh any object of weight in range of 40'''

from __future__ import print_function


def factors():
    factor_set = ((i, j, k, l) for i in [-1, 0, 1] for j in [-1, 0, 1] for k in [-1, 0, 1] for l in [-1, 0, 1])
    for factor in factor_set:
        yield factor


def memoize(f):
    results = {}

    def helper(x):
        if x not in results:
            results[x] = f(x)
        return results[x]

    return helper


@memoize
def linear_combination(n):
    weights = (1, 3, 9, 27)

    for factor in factors():
        sum = 0
        for i in range(len(factor)):
            sum += weights[i] * factor[i]
        if sum == n:
            return factor


def weighs(n):
    weights = (1, 3, 9, 27)
    scalars = linear_combination(n)
    left = ""
    right = ""

    for i in range(len(scalars)):
        if scalars[i] == -1:
            left += str(weights[i]) + " "
        elif scalars[i] == 1:
            right += str(weights[i]) + " "
    return (left, right)


res = weighs(20)
print ('Weights on the left are : ' + res[0])
print ('Weights on the right are : ' + res[1])
