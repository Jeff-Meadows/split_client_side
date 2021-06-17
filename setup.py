
from os.path import dirname, join
import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    # pylint:disable=attribute-defined-outside-init

    user_options = [('pytest-args=', 'a', "Arguments to pass to pytest")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = None

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # Do the import here, once the eggs are loaded.
        import pytest  # pylint:disable=import-outside-toplevel
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


def main():
    base_dir = dirname(__file__)
    install_requires = [
        'splitio_client',
        'sqlalchemy',
    ]
    test_requires = [
        'pycodestyle',
        'pylint',
        'tox',
        'pytest>=6.2.3',
        'pytest-mock>=3.5.1',
    ]
    extra_requires = {}
    extra_requires['test'] = test_requires
    setup(
        name='split_client_side',
        version='0.0.1',
        description='Unofficial client-side Split SDK',
        long_description=open(join(base_dir, 'README.md'), encoding='utf-8').read(),
        author='Box',
        author_email='oss@box.com',
        url='http://opensource.box.com',
        packages=find_packages(exclude=['test', 'test*', '*test', '*test*']),
        install_requires=install_requires,
        extras_require=extra_requires,
        tests_require=test_requires,
        cmdclass={'test': PyTest},
        classifiers=[],
        keywords='split client sdk',
        license='Apache Software License, Version 2.0, http://www.apache.org/licenses/LICENSE-2.0',
    )


if __name__ == '__main__':
    main()
