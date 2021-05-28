mkdir -p data
mkdir -p clean_data
mkdir -p normalized_data
mkdir -p labeled_data

./preprocess.py 
./cluster.py 
./rank.py 
./mapping.py
