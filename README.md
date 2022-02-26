# itcs_assessment

#### 1 - Dockerfile used to build pyspark image
        docker build -t pyspark --no-cache .

#### 2 - run the follwoing command in powershell (windows) to start container and mount current working dir to clusterVol
        docker run -it -v ${pwd}:/clusterVol --name container1 pyspark

#### 3 - cd to clusterVol1 and submit pyspark app
        cd /clusterVol/ && spark-submit transformation_script.py

#### 4 - resultes been written in dir analysis_csv_files on host disk
