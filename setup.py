from setuptools import setup, find_packages

setup(
    name="Xcute",
    version="0.7.8",
    packages=find_packages(),
    scripts=["x"],
    install_requires=["gevent", "requests", "termcolor", "progressbar", "gnureadline"],
    author="Pavel Vorobyov",
    author_email="aquavitale@yandex.ru",
    description="Parallel execution command-line tool",
    license="MIT",
    keywords="executer xcute conductor",
    url="https://github.com/viert/xcute"
)
