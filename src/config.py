import random

DATAPTH = "../data"

def gen_timeout(lower_bound:int=150,upper_bound:int=300):
    return random.randint(lower_bound,upper_bound)