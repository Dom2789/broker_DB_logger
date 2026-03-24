from src.Config import Config
from src.paramters import Parameters


def main():
    print("Hello from broker-db-logger!")
    config = Config("/Users/dom/prog/broker_DB_logger/config.txt")
    print(config)
    ipbroker = config.get_item("IPbroker")
    port = config.get_item("Port")
    path = config.get_item("Path")
    para_topic_1 = Parameters(path, config.get_item("DB1"), ipbroker, port, config.get_item("Topics1"))
    para_topic_2 = Parameters(path, config.get_item("DB2"), ipbroker, port, config.get_item("Topics2"))
    para_topic_3 = Parameters(path, config.get_item("DB3"), ipbroker, port, config.get_item("Topics3"))
    print(para_topic_1)
    print(para_topic_2)
    print(para_topic_3)

if __name__ == "__main__":
    main()
