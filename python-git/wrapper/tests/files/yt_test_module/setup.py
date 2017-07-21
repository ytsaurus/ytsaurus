from setuptools import setup

def main():
    setup(
        name = "yt-test-module",
        version = "1.0",
        packages = ["module_in_egg"],
    )

if __name__ == "__main__":
    main()
