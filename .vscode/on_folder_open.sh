source .venv/bin/activate
pip install -r requirements.txt
python -m ipykernel install --user --name=.venv --display-name "Python (.venv)"
