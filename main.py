from src.Config import Config
from src.paramters import Parameters
from src.mqtt import MQTT
import src.logger as lg
import argparse

def main():
    print("Hello from broker-db-logger!")
    config_file = "config.txt"

    # parsing command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-c")
    args = parser.parse_args()
    if args.c is not None:
        config_file = args.c

    # parsing config-file
    config = Config(config_file)
    print(config)

    # logging
    lg.setup_logging(config.get_item("PWDlog"), name_logfile="DB_logger_", add_date_to_name=True)

    # set up instances for different topics
    ipbroker = config.get_item("IPbroker")
    port = int(config.get_item("Port"))
    path = config.get_item("Path")
    para_topic_1 = Parameters(path, config.get_item("DB1"), ipbroker, port, config.get_item("Topics1"))
    para_topic_2 = Parameters(path, config.get_item("DB2"), ipbroker, port, config.get_item("Topics2"))
    para_topic_3 = Parameters(path, config.get_item("DB3"), ipbroker, port, config.get_item("Topics3"))
    print(para_topic_1)
    print(para_topic_2)
    print(para_topic_3)

    # start
    instances = [MQTT(para_topic_1), MQTT(para_topic_2), MQTT(para_topic_3)]
    clients = [mqtt_instance.subscribe() for mqtt_instance in instances]
    # keep main thread alive
    input("Press Enter to stop...")
    for c in clients:
        c.loop_stop()

if __name__ == "__main__":
    main()
