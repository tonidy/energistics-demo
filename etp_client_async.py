import sys
from base64 import b64encode
from threading import Thread
import time
import asyncio
import io
import avro.protocol
from avro.io import DatumWriter
from autobahn.asyncio.websocket import WebSocketClientProtocol, WebSocketClientFactory

from collections import namedtuple

from avro.schema import Parse as schema_parse
from avro.protocol import Parse as protocol_parse


class ETPClientProtocol(WebSocketClientProtocol):
    def __init__(self):
        self.messageMaps = {}
        self.messageHeaderSchema = schema_parse(
            open("Schemas/Energistics/Datatypes/MessageHeader.avsc").read()
        )
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

    async def onOpen(self):
        print("WebSocket connection open.")

        self.factory.messageMaps = self.messageMaps
        self.factory.messageHeaderSchema = self.messageHeaderSchema
        self.protocolsMessage = self.protocolsMessage
        self.send_etp_message(
            self.create_request_session_message(),
            "Energistics.Protocol.Core.RequestSession",
        )
        await asyncio.sleep(2)

    def send_etp_message(self, message, messageType):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)

        # need to send based on the message type -> cache this messageType from dictionary
        header = {
            "protocol": 0,
            "messageType": 1,
            "correlationId": 0,
            "messageId": 1,
            "messageFlags": 0,
        }

        # write header
        writer = avro.io.DatumWriter(self.factory.messageHeaderSchema)
        writer.write(header, encoder)

        # write body
        writer = avro.io.DatumWriter(self.factory.messageMaps[messageType])
        writer.write(message, encoder)

        self.sendMessage(bytes_writer.getvalue(), True)

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {}".format(reason))

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {0} bytes".format(len(payload)))
            header, message = self.decode(payload)
            print("header: {}".format(header))
            print("message: {}".format(message))
        else:
            print("Text message received: {0}".format(payload.decode("utf8")))

    def decode(self, payload):
        bytes_reader = io.BytesIO(payload)
        decoder = avro.io.BinaryDecoder(bytes_reader)

        # read header
        reader = avro.io.DatumReader(self.messageHeaderSchema)
        header = reader.read(decoder)

        message_header = namedtuple("MessageHeader", list(header.keys()))(
            *list(header.values())
        )

        reader = avro.io.DatumReader(
            self.protocolsMessage[message_header.protocol][message_header.messageType]
        )
        body = reader.read(decoder)

        message_body = namedtuple("MessageBody", list(body.keys()))(
            *list(body.values())
        )

        return message_header, message_body

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
                        "patch": 0,
                    },
                    "role": "producer",
                    "protocolCapabilities": {},
                },
                {
                    "protocol": 3,
                    "protocolVersion": {
                        "major": 1,
                        "minor": 1,
                        "revision": 0,
                        "patch": 0,
                    },
                    "role": "store",
                    "protocolCapabilities": {},
                },
                {
                    "protocol": 4,
                    "protocolVersion": {
                        "major": 1,
                        "minor": 1,
                        "revision": 0,
                        "patch": 0,
                    },
                    "role": "store",
                    "protocolCapabilities": {},
                },
                {
                    "protocol": 5,
                    "protocolVersion": {
                        "major": 1,
                        "minor": 1,
                        "revision": 0,
                        "patch": 0,
                    },
                    "role": "store",
                    "protocolCapabilities": {},
                },
                {
                    "protocol": 6,
                    "protocolVersion": {
                        "major": 1,
                        "minor": 1,
                        "revision": 0,
                        "patch": 0,
                    },
                    "role": "store",
                    "protocolCapabilities": {},
                },
            ],
            "supportedObjects": [],
        }

        return requestSession


if __name__ == "__main__":
    headers = {
        "Authorization": "Basic {}".format(
            b64encode(b"ilab.user:n@6C5rN!").decode("utf-8")
        )
    }

    factory = WebSocketClientFactory(
        "wss://witsmlstudio.pds.software/staging/api/etp",
        headers=headers,
        protocols=["energistics-tp"],
    )
    factory.protocol = ETPClientProtocol
    # self.factory.on_etp_message = self.on_etp_message

    loop = asyncio.get_event_loop()
    coro = loop.create_connection(factory, "witsmlstudio.pds.software", 80)
    loop.run_until_complete(coro)
    loop.run_forever()
    loop.close()
