import sys
from base64 import b64encode
from threading import Thread
import time

import avro.protocol
import io
from avro.io import DatumWriter
from autobahn.twisted.websocket import WebSocketClientProtocol, WebSocketClientFactory, connectWS
from twisted.python import log
from twisted.internet import reactor
from collections import namedtuple

from avro.schema import parse as schema_parse
from avro.protocol import parse as protocol_parse


class ETPClient(Thread):
    def __init__(self, url, username, password):
        super(ETPClient, self).__init__()
        log.startLogging(sys.stdout)

        headers = {
            'Authorization': 'Basic {}'.format(b64encode(b'ilab.user:n@6C5rN!').decode("utf-8"))
        }

        factory = WebSocketClientFactory("wss://witsmlstudio.pds.software/staging/api/etp", headers=headers,
                                         protocols=["energistics-tp"])
        self.factory = factory

        self.factory.protocol = ETPClientProtocol

        self.factory.on_etp_message = self.on_etp_message

    def on_etp_message(self, header, body):
        print(header)
        print(body)

    def run(self):
        reactor.run(installSignalHandlers=0)

    def connect(self):
        self.daemon = True
        connectWS(self.factory)
        self.start()

    def create_request_session_message(self):
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

        return requestSession

    def request_session(self):
        print("request session")
        self.send_etp_message(self.create_request_session_message(), "Energistics.Protocol.Core.RequestSession")

    def send_etp_message(self, message, messageType):
        self.factory.send_message(message, messageType)


class ETPClientProtocol(WebSocketClientProtocol):
    def __init__(self):
        self.messageMaps = {}
        self.messageHeaderSchema = schema_parse(open("Schemas/Energistics/Datatypes/MessageHeader.avsc").read())
        self.protocolsMessage = {}

        schema_path = "etp.avpr"

        protocol = protocol_parse(open(schema_path).read())

        for type in protocol.types:
            if "protocol" in type.other_props:
                protocol_no = int(type.other_props["protocol"])
                message_type = int(type.other_props["messageType"])
                if not protocol_no in self.protocolsMessage:
                    self.protocolsMessage[protocol_no] = {}
                self.protocolsMessage[protocol_no][message_type] = type
                self.messageMaps[type.namespace + "." + type.name] = type

    def onConnect(self, response):
        print("Connected to Server: {}".format(response.peer))

    def onOpen(self):
        print("WebSocket connection open.")

        # self.factory.send_message = self.sendMessage
        self.factory.send_message = self.send_etp_message

        self.factory.messageMaps = self.messageMaps
        self.factory.messageHeaderSchema = self.messageHeaderSchema
        self.protocolsMessage = self.protocolsMessage

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {}".format(reason))

    def send_etp_message(self, message, messageType):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)

        # need to send based on the message type -> cache this messageType from dictionary
        header = {"protocol":0,"messageType":1,"correlationId":0,"messageId":1,"messageFlags":0}

        # write header
        writer = avro.io.DatumWriter(self.factory.messageHeaderSchema)
        writer.write(header, encoder)

        # write body
        writer = avro.io.DatumWriter(self.factory.messageMaps[messageType])
        writer.write(message, encoder)

        self.sendMessage(bytes_writer.getvalue(), True)

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
        reader = avro.io.DatumReader(self.messageHeaderSchema)
        header = reader.read(decoder)

        message_header = namedtuple("MessageHeader", header.keys())(*header.values())

        reader = avro.io.DatumReader(self.protocolsMessage[message_header.protocol][message_header.messageType])
        body = reader.read(decoder)

        message_body = namedtuple("MessageBody", body.keys())(*body.values())

        return message_header, message_body

    def on_etp_message(self, header, message):
        self.factory.on_etp_message(header, message)


if __name__ == '__main__':
    etpClient = ETPClient("url", "username", "password")

    etpClient.connect()

    time.sleep(3)

    openSession = etpClient.request_session()

    time.sleep(5)
