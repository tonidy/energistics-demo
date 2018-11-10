import pandas as pd
from io import StringIO


class EnergyConnect():
    def __init__(self, connType, url, username, password):
        self.connType = connType
        self.url = url
        self.username = username
        self.password = password

        # need to create factory for this one.
        if connType == "etp":
            # create ETP connection here
            print("# create ETP connection here")
        elif connType == "witsml":
            print("# create WITSML connection here")

    def set_parent_path(self, param):
        pass

    def get_data_frame(self, *argv):
        # need to handle depth index and time index
        data = 'ROP,WOB,HKLI\n1,2,3\n1,2,3\n1,2,3'

        # for ETP connection need to activate protocol 2 for data frame and protocol 1
        # describe the channels first and then get range

        # for WITSML connection need to get log object
        # a lot of alternative here based on current WITSML 2.0 interaction

        # need to do data alignment for ETP
        return pd.read_csv(StringIO(data))

    def subscribe(self, sensors, handle_data):
        #subscribe to spesific index value from data
        #
        pass


# connect to etp server
client = EnergyConnect("etp", "wss://witsmlstudio.pds.software/staging/api/etp", "ilab.user", "n@6C5rN!")

# connect to witsml server
client = EnergyConnect("witsml", "https://witsmlstudio.pds.software/staging/api/soap", "ilab.user", "n@6C5rN!")

# set default parent path and location
client.connect("eml://witsml20/Log(9f811c02-1aad-41e1-86d0-7f082ddbd0db)")

# get sensors reading
df = client.get_data_frame("ROP", "WOB", "HKLI")

# print dataframe
df

# do keras training and utilize another machine learning library here .

# do sckitlearn model etc

# store model for prediction

# create library of drilling formulas

def handle_data(data):
    print(data)

# subscribe to realtime data, need to
client.subscribe(["ROP", "WOB", "HKLI"], handle_data)

