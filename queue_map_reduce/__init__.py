from . import queue
from . import utils
from . import network_file_system
from . import pool
from .pool import Pool

import warnings
warnings.warn(
    "This package 'queue_map_reduce' is no longer maintained. "
    "It got replaced by https://pypi.org/project/pypoolparty.",
    category=DeprecationWarning,
)
