h1 python3 ../../../../DiscoveryAppln.py -l 10 -a "10.0.0.1" -p "5555" -P 1 -s 1 -c config.ini > discovery.out 2>&1 &
h2 python3 ../../../../PublisherAppln.py -l 10 -d "10.0.0.1:5555" -a "10.0.0.2" -T 9 -n pub1 -c config.ini > pub1.out 2>&1 &
h3 python3 ../../../../BrokerAppln.py -l 10 -d "10.0.0.1:5555" -a "10.0.0.3" -T 9 -n broker -c config.ini > broker.out 2>&1 &
h4 python3 ../../../../SubscriberAppln.py -l 10 -d "10.0.0.1:5555" -T 9 -n sub1 -m metrics.csv -c config.ini > sub1.out 2>&1 &

