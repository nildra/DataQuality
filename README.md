# DataQuality

## How to setup IOTDB
Install Apache IoTDB [here](https://iotdb.apache.org/UserGuide/V1.2.x/QuickStart/QuickStart.html)
            
    run the serveur:
        in /sbin folder: ./start-standalone.sh

    run the client:
        in /sbin folder: ./start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root

## How to run the demo
    1) Add data from a csv file into IoTDB:
        python3 csv2tsfile.py

    2) Run the demo:
        python3 demo.py
