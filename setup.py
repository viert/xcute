from setuptools import setup, find_packages

setup(
    name="Xcute",
    version="0.9.0",
    packages=find_packages(),
    scripts=["x"],
    install_requires=["gevent", "requests", "termcolor", "progressbar", "gnureadline", "pyparsing"],
    author="Pavel Vorobyov",
    author_email="aquavitale@yandex.ru",
    description="Parallel execution command-line tool",
    license="MIT",
    keywords="executer xcute conductor",
    url="https://github.com/viert/xcute"
)
