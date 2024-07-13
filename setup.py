from setuptools import setup, find_packages


setup(
    name="ARDA_TRADE_2",
    packages=find_packages(where= '.'),   #['analysis', 'configs', 'helpers','services','ext_services'],
    package_data={
        # If any package contains *.txt or *.yaml files, include them:
        "": ["*.txt", "*.yaml","*.ipynb"]
    },
    include_package_data=True,
)
