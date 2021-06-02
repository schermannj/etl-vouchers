# -*- coding: utf-8 -*-
from setuptools import setup

packages = ['etl_vouchers']

package_data = {'': ['*']}

install_requires = ['great-expectations>=0.13.19,<0.14.0',
                    'invoke>=1.5.0,<2.0.0',
                    'pandas>=1.2.4,<2.0.0']

setup_kwargs = {
    'name': 'etl-vouchers',
    'version': '0.1.0',
    'description': '',
    'long_description': None,
    'author': 'vsheruda',
    'author_email': 'vladyslav.sheruda@gmail.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': None,
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'python_requires': '==3.7.2',
}

setup(**setup_kwargs)
