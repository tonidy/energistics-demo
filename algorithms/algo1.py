from algorithms.realtime_algorithm import RealtimeAlgorithm


class Algorithm1(RealtimeAlgorithm):
    def initialize(self):
        self.subscribe("HKLI", "BITDEPTH")

    # need to find a way to get historical data from backend
    # need to find a way to push the data to alerting dashboard 
    # simple curve visualization
    def handle(self, data):
        print(data.sensor)
        print(data.index)
        print(data.value)
