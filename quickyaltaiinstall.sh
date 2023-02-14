virtualenv -p python3.8 yaltaienv
yaltaienv/bin/pip install yaltai==0.1.0 --extra-index-url https://download.pytorch.org/whl/cu113

virtualenv -p python3.8 krakenv
krakenv/bin/pip install kraken==4.3.1 --extra-index-url https://download.pytorch.org/whl/cu113