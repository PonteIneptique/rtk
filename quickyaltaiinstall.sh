virtualenv -p python3.8 yaltaidenv
yaltaienv/bin/pip install yaltai numpy==1.23.1 fast_deskew shapely==1.8.4 --extra-index-url https://download.pytorch.org/whl/cu113

virtualenv -p python3.8 krakenv
krakenv/bin/pip install kraken numpy==1.23.1  --extra-index-url https://download.pytorch.org/whl/cu113