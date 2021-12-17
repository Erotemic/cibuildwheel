
import sys
from setuptools import setup, Extension



libraries = []
if sys.platform.startswith('linux'):
    libraries.extend(['m', 'c'])

setup(
    ext_modules=[Extension(
        'spam',
        sources=['spam.c'],
        libraries=libraries,
    )],
    
)