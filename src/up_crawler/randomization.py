import random
import time

from dataclasses import dataclass

import logging
logging.basicConfig()
logger = logging.getLogger(__package__)

@dataclass
class RandomizationParams:
    """Parameters and functions related to getting around possible 403s:

    - calculation of AND EXECUTION of timeouts/waits
    - picking a random useragent from list
    """

    # let's do _ethical crawling_ (c)(tm)(r) 
    POLITE_USERAGENT = """Dear Ukrainska Pravda, I'm writing a masters thesis and \
am downloading some of your articles - contact me at pravda@serhii.net in case \
there are any issues with that! Serhii."""

    max_wait_sec: int = 2
    wait_eps: int = 3

    # TODO - automate headers trhough latest-user-agents package etc. IF NEEDED
    user_agents: tuple[str] = (POLITE_USERAGENT,)

    def get_useragent(self):
        return random.choice(self.user_agents)

    def get_wait_time(self):
        return self._calc_rand_wait(
            min_sec=0, max_sec=self.max_wait_sec, eps=self.wait_eps
        )

    def random_wait(self):
        time.sleep(self.get_wait_time())

    @staticmethod
    def _calc_rand_wait(min_sec: int = 0, max_sec: int = 12, eps: float = 2):
        wait_time = random.randint(min_sec, max_sec)
        wait_time = _slightly_change_num(wait_time, eps=eps, only_positive=True)
        logger.debug(
            f"waiting for {wait_time:.2f} seconds ({min_sec=}, {max_sec=}, {eps=})"
        )
        return wait_time



def _slightly_change_num(num: float, eps=0.01, only_positive: bool = True) -> float:
    """Randomly change value of num to within num-eps>new_num>num+eps"""
    res = num - eps + random.random() * eps * 2
    if only_positive and res < 0:
        res = 0
    return res


## CLI

def _parse_timeout(args) -> RandomizationParams:
    """ Parse CLI arguments arguments. """
    if args.timeout == -1:
        rw = RandomizationParams(max_wait_sec=0, wait_eps=0)
    else:
        rw = RandomizationParams(max_wait_sec=args.timeout)
    return rw
