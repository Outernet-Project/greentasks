import os
from setuptools import setup, find_packages


def read(fname):
    """ Return content of specified file """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='greentasks',
    description=('A simple task scheduler, built on top of gevent.'),
    keywords='asynchronous task scheduler',
    version='2.0.post1',
    author='Outernet Inc',
    author_email='apps@outernet.is',
    license='BSD',
    url='https://github.com/Outernet-Project/greentasks',
    long_description=read('README.rst'),
    packages=find_packages(),
    install_requires=['gevent'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Topic :: Utilities',
    ],
)
