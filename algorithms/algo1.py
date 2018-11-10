from algorithms.realtime_algorithm import RealtimeAlgorithm


class Algorithm1(RealtimeAlgorithm):
    def initialize(self):
        self.nn_model = {}
        self.nn_model.initialize()
        self.threshold = 100
        self.subscribe("HKLI", "BITDEPTH")
        self.channel_manager = {}
        self.channel_manager.create("eml://witsml20/channel(something)")

    # need to find a way to get historical data from backend
    # need to find a way to push the data to alerting dashboard 
    # simple curve visualization
    def handle_tick(self, data):

        startIdx = {}
        endIdx = {}

        df = self.history('ROP', 'WOB', 'HKLI', startIdx, endIdx)

        print(data.sensor)
        print(data.index)
        print(data.value)


    # need to handle alignment algorithm, use last value, interpolate etc
    def handle_frame(self, data_frame):
        value = self.nn_model.predict(data_frame.values)
        if value > self.threshold:
            self.alert() # call API for alerting


        # store value for neural network model
