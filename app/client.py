import datetime
import gzip
import io
import json
import time
import uuid
from urllib.parse import urljoin

import pandas as pd

from app.session import Session
from config import logger


class Client:

    HOST = "https://api.bloomberg.com"

    def __init__(
        self,
        instruments: list,
        fields: list,
        session: Session,
        reply_timeout_min: int,
        identifier_type: str,
    ):
        self.instruments = instruments
        self.fields = fields
        self.reply_timeout_min = reply_timeout_min
        self.identifier_type = identifier_type
        self.catalog_id = None
        self.catalog_url = None
        self.request_id = None
        self.request_url = None
        self.output_key = None
        self.output_url = None
        self.session = session
        self.session_id = self._generate_session_id()
        self._get_catalog_id()

    def data_request(self):
        url = urljoin(self.catalog_url, "requests/")
        request_payload = self._get_request_payload()
        response = self.session.post(url, json=request_payload)
        self.request_url = urljoin(self.HOST, response.headers["Location"])
        self.request_id = response.json()["request"]["identifier"]

    def listen(self):
        if self.__listen():
            return self.__download()

        return

    def __listen(self):
        url = urljoin(self.catalog_url, "content/responses/")
        params = {
            "prefix": self.session_id,
            "requestIdentifier": self.request_id,
        }
        now = datetime.datetime.utcnow()
        reply_timeout = datetime.timedelta(minutes=self.reply_timeout_min)
        expiration_timestamp = now + reply_timeout
        while now < expiration_timestamp:
            response = self.session.get(url, params=params)
            data = response.json()["contains"]
            if len(data):
                output = data[0]
                logger.info("Response listing:\n%s", json.dumps(output, indent=2))
                self.output_key = output["key"]
                self.output_url = urljoin(
                    self.catalog_url, f"content/responses/{self.output_key}"
                )
                return True
            else:
                time.sleep(30)
        else:
            logger.info(
                f"Response not received within {self.reply_timeout_min} minutes. Exiting."
            )
            return False

    def __download(self):
        with self.session.get(self.output_url, stream=True) as response:
            output_filename = self.output_key
            if "content-encoding" in response.headers:
                if not response.headers["content-encoding"] == "gzip":
                    raise RuntimeError(
                        "Unsupported content encoding received in the response"
                    )

            uncompressed = gzip.GzipFile(fileobj=response.raw)
            data = uncompressed.read()
            df = pd.read_json(io.BytesIO(data))

        logger.info("File downloaded: %s", output_filename)
        return df

    def _get_catalog_id(self):
        url = urljoin(self.HOST, "/eap/catalogs/")
        response = self.session.get(url)
        catalogs = response.json()["contains"]
        for catalog in catalogs:
            if catalog["subscriptionType"] == "scheduled":
                self.catalog_id = catalog["identifier"]
                self.catalog_url = urljoin(
                    self.HOST, f"/eap/catalogs/{self.catalog_id}/"
                )
                break
        else:
            logger.error("Scheduled catalog not in %r", response.json()["contains"])
            raise RuntimeError("Scheduled catalog not found")

    def _get_request_payload(self):
        universe = self._get_universe_payload()
        fieldlist = self._get_fieldlist_payload()
        trigger = self._get_trigger()
        request_payload = {
            "@type": "DataRequest",
            "name": self.session_id,
            "description": "BBGCLIENT",
            "universe": {"@type": "Universe", "contains": universe},
            "fieldList": {"@type": "DataFieldList", "contains": fieldlist},
            "trigger": trigger,
            "formatting": {
                "@type": "MediaType",
                "outputMediaType": "application/json",
            },
            # 'terminalIdentity': {
            #     '@type': 'BlpTerminalIdentity',
            #     'userNumber': 12345678,
            #     'serialNumber': 123,
            #     'workStation': 12
            # }
        }
        return request_payload

    def _get_universe_payload(self):
        payload = [
            self._get_universe_structure(instrument) for instrument in self.instruments
        ]
        return payload

    def _get_fieldlist_payload(self):
        payload = [self._get_fieldlist_structure(field) for field in self.fields]
        return payload

    def _get_trigger(self):
        return urljoin(self.catalog_url, "triggers/executeNow")

    def _get_universe_structure(self, id: str):
        return {
            "@type": "Identifier",
            "identifierType": self.identifier_type,
            "identifierValue": id,
        }

    @staticmethod
    def _get_fieldlist_structure(field):
        return {"mnemonic": field}

    @staticmethod
    def _generate_session_id():
        return f"pa{str(uuid.uuid4())[:8]}"
