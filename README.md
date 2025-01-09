# DataQuality

## How to run IOTDB 
    run the serveur:
        ./start-standalone.sh

    run the client:
        ./start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root

## How to run the demo
    1) Add data from a csv file into IoTDB:
        python3 csv2tsfile.py

    2) Run the demo:
        python3 demo.py
