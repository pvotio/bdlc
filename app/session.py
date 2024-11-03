import datetime

from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session

from config import logger


class Session(OAuth2Session):
    """Session class for making requests to a DL REST API
    using OAuth2 authentication."""

    def __init__(self, client_secret, *args, **kwargs):
        """
        Initialize a Session instance.
        """
        self.client_secret = client_secret
        super().__init__(*args, **kwargs)

    def request_token(self):
        """
        Fetch an OAuth2 access token by making a request to the token endpoint.
        """
        oauth2_endpoint = "https://bsso.blpprofessional.com/ext/api/as/token.oauth2"
        self.token = self.fetch_token(
            token_url=oauth2_endpoint,
            client_secret=self.client_secret,
        )

    def request(self, *args, **kwargs):
        """
        Override the parent class method to handle TokenExpiredError
        by refreshing the token.
        :return: response object from the API request
        """
        try:
            response = super().request(*args, **kwargs)
        except TokenExpiredError:
            self.request_token()
            response = super().request(*args, **kwargs)

        return response

    def send(self, request, **kwargs):
        """
        Override the parent class method to log request and response information.
        :param request: prepared request object
        :return: response object from the API request
        """
        logger.debug(
            "Request being sent to HTTP server: %s, %s",
            request.method,
            request.url,
        )

        response = super().send(request, **kwargs)

        logger.debug("Response status: %s", response.status_code)
        logger.debug("Response x-request-id: %s", response.headers.get("x-request-id"))

        if response.ok:
            if not kwargs.get("stream"):
                logger.debug("Response content: %s", response.text)
        else:
            raise RuntimeError(
                "\n\tUnexpected response status code: {c}\nDetails: {r}".format(
                    c=str(response.status_code), r=response.json()
                )
            )

        return response


def check_credentials(credentials):
    expires_in = (
        datetime.datetime.fromtimestamp(credentials["expiration_date"] / 1000)
        - datetime.datetime.utcnow()
    )
    if expires_in.days < 0:
        logger.warning("Credentials expired %s days ago", expires_in.days)
    elif expires_in < datetime.timedelta(days=30):
        logger.warning("Credentials expiring in %s", expires_in)


def get_session(credentials):
    check_credentials(credentials)
    client = BackendApplicationClient(client_id=credentials["client_id"])
    session = Session(client_secret=credentials["client_secret"], client=client)
    session.headers["api-version"] = "2"
    session.request_token()
    return session
