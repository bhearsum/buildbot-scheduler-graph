from setuptools import setup

from buildbot_scheduler_graph import __version__

setup(
    name='buildbot-scheduler-graph',
    version=__version__,
    description='A tool for generating digraphs of Buildbot Builders and Schedulers',
    author='Ben Hearsum',
    py_modules=['buildbot_scheduler_graph'],
    entry_points={
        'console_scripts': ['buildbot-scheduler-graph = buildbot_scheduler_graph:main'],
    },
    zip_safe=False,
    install_requires=[
        "pyparsing==1.5.6",
        "pydot>=1.0.28",
    ],
)
