import enum


class Stage(str, enum.Enum):
    STABLE = "stable"  # Main production installation on production YT cluster.
    PRESTABLE = "prestable"
    DEV = "dev"  # For developing on test YT cluster (pythia, zeno, etc).
    TEST = "test"  # For local and CI tests.
