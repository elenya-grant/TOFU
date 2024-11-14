from pathlib import Path
from setuptools import setup, find_packages

NAME = "TOFU"
DESCRIPTION = "playground for onsite analysis"
URL = "https://github.com/elenya-grant/TOFU.git"
EMAIL = "elenya.grant@nrel.gov"
AUTHOR = "NREL"
REQUIRES_PYTHON = ">=3.8.0"
VERSION = 1.0
ROOT = Path(__file__).parent


base_path = Path("tofu")
# package_data_files = [*ROOT.glob("tools/2-6 digit_2022_Codes.xlsx")]
# package_data_files = [
#     *ROOT.glob("./data/tl_2023_us_state/*"),
#     *ROOT.glob("./data/tl_2023_us_uac20/*"),
#     *ROOT.glob("./data/tl_2023_us_state/*")
# ]

# package_data = {
#     "tofu": [str(file.relative_to(base_path)) for file in package_data_files],
# }

setup(
    name=NAME,
    version=VERSION,
    url=URL,
    description=DESCRIPTION,
    # long_description=(base_path.parent / "RELEASE.md").read_text(),
    # long_description_content_type='text/markdown',
    license='Apache Version 2.0',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    packages=find_packages(),
    # package_data=package_data,
    include_package_data=True,
    install_requires=(base_path.parent / "requirements.txt").read_text().splitlines(),
    tests_require=['pytest', 'pytest-subtests', 'responses']
)