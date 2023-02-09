from dataclasses import dataclass

SKIP_NOT_COMPILED: bool = False


@dataclass
class Flags:
    skip_not_compiled: bool = False


_DEFAULT_FLAGS = Flags()


def set_flags(flags: Flags) -> None:
    global SKIP_NOT_COMPILED
    SKIP_NOT_COMPILED = flags.skip_not_compiled


def reset_flags() -> None:
    set_flags(_DEFAULT_FLAGS)
