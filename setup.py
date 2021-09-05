import setuptools


REQUIRED_PACKAGES = [
    'pandas',
    'apache-beam[gcp]'
]

setuptools.setup(
    name='dataflow-template',
    version='',
    packages=setuptools.find_packages(),
    install_requires=REQUIRED_PACKAGES,
    url='',
    license='',
    author='',
    author_email='',
    description=''
)

