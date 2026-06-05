from metaToImport import TestClass  # type: ignore[import-not-found]


def main() -> None:
    test = TestClass()
    success = test.someFunction()
    if not success:
        raise RuntimeError("Failed to collect debug connection in imported class")


if __name__ == "__main__":
    main()
