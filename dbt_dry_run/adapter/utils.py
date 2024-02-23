from dbt.cli import resolvers


def default_profiles_dir() -> str:
    return resolvers.default_profiles_dir().as_posix()
