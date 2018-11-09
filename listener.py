import importlib

from confluent_kafka import Consumer, KafkaError
import json


def load_class(full_class_string):
    """
    dynamically load a class from a string
    """

    class_data = full_class_string.split(".")
    module_path = ".".join(class_data[:-1])
    class_str = class_data[-1]

    module = importlib.import_module(module_path)
    # Finally, we retrieve the Class
    return getattr(module, class_str)

# need to think about partition, consumer, consumer groups

c = Consumer({
    'bootstrap.servers': 'cloudera-01.tambunan.com',
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest'
})

c.subscribe(['lizzy'])

algorithm = load_class("algorithms.algo1.Algorithm1")
instance = algorithm()
instance.initialize()

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    row = msg.value().decode('utf-8')
    rowObj = json.loads(row)

    instance.handle(rowObj)
c.close()


