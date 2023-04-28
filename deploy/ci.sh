set -ex

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd $SCRIPTPATH/../src
export MYPYPATH=..

python3 -m mypy --strict -m src.main

python3 -m pytest tests/test_services.py
python3 -m pytest tests/test_controllers.py
python3 -m pytest tests/test_serializers.py