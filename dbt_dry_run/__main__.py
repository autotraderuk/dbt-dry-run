from dbt_dry_run import cli


def main() -> None:
    exit(cli.run())


if __name__ == "__main__":
    main()
