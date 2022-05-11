from random import SystemRandom as _SystemRandom


class SystemRandom(_SystemRandom):
    """Alternative implementation of SystemRandom that supports pickle and deepcopy."""
    getstate = setstate = lambda self, *args, **kwargs: None
