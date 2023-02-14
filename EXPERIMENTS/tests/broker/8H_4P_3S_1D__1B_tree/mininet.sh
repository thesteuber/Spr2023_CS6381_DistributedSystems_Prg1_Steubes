h1 python3 ../../../../DiscoveryAppln.py -l 10 -a "10.0.0.1" -p "5555" -P 4 -s 2 -c config.ini > discovery.out 2>&1 &
h2 python3 ../../../../PublisherAppln.py -l 10 -d "10.0.0.1:5555" -a "10.0.0.2" -T 9 -n pub1 -c config.ini > pub1.out 2>&1 &
h3 python3 ../../../../PublisherAppln.py -l 10 -d "10.0.0.1:5555" -a "10.0.0.3" -T 9 -n pub2 -c config.ini > pub2.out 2>&1 &
h4 python3 ../../../../PublisherAppln.py -l 10 -d "10.0.0.1:5555" -a "10.0.0.4" -T 9 -n pub3 -c config.ini > pub3.out 2>&1 &
h5 python3 ../../../../PublisherAppln.py -l 10 -d "10.0.0.1:5555" -a "10.0.0.5" -T 9 -n pub4 -c config.ini > pub4.out 2>&1 &
h6 python3 ../../../../BrokerAppln.py -l 10 -d "10.0.0.1:5555" -a "10.0.0.6" -T 9 -n broker -c config.ini > broker.out 2>&1 &
h7 python3 ../../../../SubscriberAppln.py -l 10 -d "10.0.0.1:5555" -T 1 -n sub1 -m metrics.csv -c config.ini > sub1.out 2>&1 &
h8 python3 ../../../../SubscriberAppln.py -l 10 -d "10.0.0.1:5555" -T 2 -n sub2 -m metrics.csv -c config.ini > sub2.out 2>&1 &