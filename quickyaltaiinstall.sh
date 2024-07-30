virtualenv -p python3.10 yaltaienv
yaltaienv/bin/pip install YALTAi --extra-index-url https://download.pytorch.org/whl/cpu

virtualenv -p python3.10 krakenv
krakenv/bin/pip install kraken --extra-index-url https://download.pytorch.org/whl/cpu
