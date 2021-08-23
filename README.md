# bv_mapping
1. Add these to the betterview environment: `conda install dask tqdm`
2. `mkdir data`
3. `mkdir zipcodes`
4. download zipcode shapes from: https://drive.google.com/drive/folders/1nHB9hMFcRdyUOKpaK1h83uGvSHK8ewPo?usp=sharing
5. `mv *.csv ./zipcodes` 
6. Run `property_to_zip.py` (runtime: approximately 10 hours)
7. Run `join_data_to_shape.py` (runtime: less than one hour)
