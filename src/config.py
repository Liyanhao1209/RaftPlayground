import random

DATAPTH = "../data"
HEARTBEAT_INTERVAL = 0.05

clusters = {
    "5555":1,
    "5556":2,
    "5557":3,
    "5558":4,
    "5559":5
}

BASE_PORT = 5000

def gen_timeout(lower_bound:float=150,upper_bound:float=300):
    return random.randint(lower_bound,upper_bound)

def get_dedicated_port(host_id: int, peer_id: int) -> int:
    return BASE_PORT + (host_id * 10) + peer_id