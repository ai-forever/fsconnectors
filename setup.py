from setuptools import setup, find_packages


def parse_requirements():
    with open('requirements.txt') as file:
        requirements = [line.strip() for line in file.readlines()]
    return requirements


if __name__ == '__main__':
    setup(
        name='fsconnectors',
        version='1.0',
        author='AI Forever',
        url='https://github.com/ai-forever/fsconnectors',
        package_dir={'': '.'},
        packages=find_packages('.'),
        install_requires=parse_requirements(),
        entrypoints={'console_scripts': ['fsconnectors=fsconnectors.s3utils:main']}
    )
