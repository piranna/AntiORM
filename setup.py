'''
Created on 18/06/2012

@author: piranna
'''

from distutils.core import setup
from os             import listdir
from os.path        import isdir, join

from antiorm import __version__


def find_packages(base):
    ret = [base]
    for path in listdir(base):
        if path.startswith('.'):
            continue
        full_path = join(base, path)
        if isdir(full_path):
            ret += find_packages(full_path)
    return ret


setup(
    name='AntiORM',
    version=__version__,

    packages=find_packages('antiorm'),

    description='The ORM that goes against all others',
    author='Jesus Leganes Combarro "Piranna"',
    author_email='piranna@gmail.com',
    download_url='https://github.com/piranna/AntiORM/zipball/master',
    long_description=open('README.mediawiki').read(),
    license='GPL',
    url='https://github.com/piranna/AntiORM',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.4',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: SQL',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development',
        'Topic :: Software Development :: Code Generators'
    ],
)
