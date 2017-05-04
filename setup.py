# encoding=
from setuptools import setup, find_packages
setup(
    name = "etlpy",
    version = "0.1",
    packages=find_packages('src'),  # 包含所有src中的包
    package_dir={'': 'src'},  # 告诉distutils包都在src下


)