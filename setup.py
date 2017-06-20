try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version = "0.0.1"

setup(
    name="kubeface",
    version=version,
    author="Tim O'Donnell",
    author_email="timodonnell@gmail.com",
    packages=["kubeface", "kubeface.commands"],
    url="https://github.com/hammerlab/kubeface",
    license="Apache License",
    description="Python parallel for loops on kubernetes",
    long_description=open('README.md').read(),
    download_url='https://github.com/hammerlab/kubeface/tarball/%s' % version,
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
    ],
    entry_points={
        'console_scripts': [
            'kubeface-copy = kubeface.commands.copy:run',
            'kubeface-run2 = kubeface.commands.run_commands:run',
            'kubeface-run-commands = kubeface.commands.run_commands:run',
            'kubeface-job = kubeface.commands.job:run',
            '_kubeface-run-task = kubeface.commands.run_task:run',
        ]
    },
    install_requires=[
        "dill>=0.2.5",
        "six",
        "numpy",
        "parse",
        "oauth2client==4.0.0",
        "google-api-python-client==1.5.5",
        "mock",
        "nose>=1.3.1",
    ]
)
