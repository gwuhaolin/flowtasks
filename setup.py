import subprocess

from setuptools import setup, find_packages

setup(
    name="flowtasks",
    version=subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode().strip(),
    packages=find_packages(),
    install_requires=[
    ],
    author="hal",
    description="Python多进程并发调度工作流",
    long_description="Python多进程并发调度工作流",
    url="https://github.com/gwuhaolin/flowtasks",
    classifiers=[
    ],
)
