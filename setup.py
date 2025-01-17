from setuptools import find_packages, setup
from typing import List


def get_requirements() -> List[str]:
    """
    This function will return list of requirements
    """

    requirement_lst: List[str] = []
    try:
        with open("requirements.txt") as file:
            # Read from the lines
            lines = file.readlines()
            # Process each line
            for line in lines:
                requirement = line.strip()
                if requirement and requirement != "-e .":
                    requirement_lst.append(requirement)
    except FileNotFoundError:
        print("requirements.txt file not found")

    return requirement_lst


setup(
    name="NetworkSecurity",
    version="0.0.1",
    author="Sushant Suki",
    author_email="sushantsukhi@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements(),
)
