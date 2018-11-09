import sys
from base64 import b64encode
import avro.protocol
import io, random
from avro.io import DatumWriter
from autobahn.twisted.websocket import WebSocketClientProtocol, WebSocketClientFactory, connectWS
from twisted.python import log
from twisted.internet import reactor
from collections import namedtuple

messageMaps = {}

messageHeaderSchema = avro.schema.Parse(open("Schemas/Energistics/Datatypes/MessageHeader.avsc").read())

# w, h = 100, 100
protocolsMessage = {}


class MyClientProtocol(WebSocketClientProtocol):
    def onError(self, err):
        print("error")

    def onConnect(self, response):
        print("Connected to Server: {}".format(response.peer))

    def onOpen(self):
        print("WebSocket connection open.")

        requestSession = {
            "applicationName": "",
            "applicationVersion": "",
            "requestedProtocols": [
                                {
                                  "protocol": 1,
                                  "protocolVersion": {
                                    "major": 1,
                                    "minor": 1,
                                    "revision": 0,
                                    "patch": 0
                                  },
                                  "role": "producer",
                                  "protocolCapabilities": {}
                                },
                                {
                                  "protocol": 3,
                                  "protocolVersion": {
                                    "major": 1,
                                    "minor": 1,
                                    "revision": 0,
                                    "patch": 0
                                  },
                                  "role": "store",
                                  "protocolCapabilities": {}
                                },
                                {
                                  "protocol": 4,
                                  "protocolVersion": {
                                    "major": 1,
                                    "minor": 1,
                                    "revision": 0,
                                    "patch": 0
                                  },
                                  "role": "store",
                                  "protocolCapabilities": {}
                                },
                                {
                                  "protocol": 5,
                                  "protocolVersion": {
                                    "major": 1,
                                    "minor": 1,
                                    "revision": 0,
                                    "patch": 0
                                  },
                                  "role": "store",
                                  "protocolCapabilities": {}
                                },
                                {
                                  "protocol": 6,
                                  "protocolVersion": {
                                    "major": 1,
                                    "minor": 1,
                                    "revision": 0,
                                    "patch": 0
                                  },
                                  "role": "store",
                                  "protocolCapabilities": {}
                                }
                              ],
            "supportedObjects": []
        }
        self.send(requestSession, "Energistics.Protocol.Core.RequestSession")

    def send(self, message, messageType):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)

        # need to send based on the message type
        header = {"protocol":0,"messageType":1,"correlationId":0,"messageId":1,"messageFlags":0}

        # write header
        writer = avro.io.DatumWriter(messageHeaderSchema)
        writer.write(header, encoder)

        # write body
        writer = avro.io.DatumWriter(messageMaps[messageType])
        writer.write(message, encoder)

        self.sendMessage(bytes_writer.getvalue(), True)

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {}".format(reason))

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {0} bytes".format(len(payload)))
            header, message = self.decode(payload)
            self.on_etp_message(header, message)
        else:
            print("Text message received: {0}".format(payload.decode('utf8')))

    def decode(self, payload):
        bytes_reader = io.BytesIO(payload)
        decoder = avro.io.BinaryDecoder(bytes_reader)

        # read header
        reader = avro.io.DatumReader(messageHeaderSchema)
        header = reader.read(decoder)

        message_header = namedtuple("MessageHeader", header.keys())(*header.values())

        reader = avro.io.DatumReader(protocolsMessage[message_header.protocol][message_header.messageType])
        body = reader.read(decoder)

        message_body = namedtuple("MessageBody", body.keys())(*body.values())

        return message_header, message_body

    def on_etp_message(self, header, message):
        print(header)
        print(message)


if __name__ == '__main__':
    log.startLogging(sys.stdout)

    headers = {
        'Authorization': 'Basic {}'.format(b64encode(b'ilab.user:n@6C5rN!').decode("utf-8"))
    }
    factory = WebSocketClientFactory("wss://witsmlstudio.pds.software/staging/api/etp", headers=headers, protocols=["energistics-tp"])

    factory.protocol = MyClientProtocol

    connectWS(factory)

    schema_path = "etp.avpr"
    protocol = avro.protocol.Parse(open(schema_path).read())

    for type in protocol.types:
        print(type.name)
        if "protocol" in type.other_props:
            protocol_no = int(type.other_props["protocol"])
            message_type = int(type.other_props["messageType"])
            if not protocol_no in protocolsMessage:
                protocolsMessage[protocol_no] = {}
            protocolsMessage[protocol_no][message_type] = type
            messageMaps[type.namespace + "." + type.name] = type


    print(protocolsMessage)
    reactor.run()
