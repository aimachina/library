mkdir aim_library
touch aim_library/__init__.py
cp -r app_factories aim_library
cp -r database aim_library
cp -r events aim_library
cp -r models_interface aim_library
cp -r services aim_library
cp -r utils aim_library
source venv/bin/activate
python setup.py sdist bdist_wheel
twine upload --repository pypi dist/*