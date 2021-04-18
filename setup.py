
from setuptools import setup, find_packages

setup(name='pbs_do',
      version='1.0',
      description='Create PBS jobs from the standard input.',
      author='Aaron Maurais',
      url='https://github.com/ajmaurais/pbs_do',
      classifiers=['Development Status :: 4 - Beta',
        'Intended Audience :: SCIENCE/RESEARCH',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        ],
      packages=find_packages(),
      package_dir={'pbs_do':'pbs_do'},
      python_requires='>=3.6.*',
      entry_points={'console_scripts': ['pbs_do=pbs_do:main']},
)


