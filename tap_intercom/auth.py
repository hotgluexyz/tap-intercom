import json
from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from hotglue_singer_sdk.helpers._util import utc_now


class IntercomOAuthAuthenticator(OAuthAuthenticator):
    """OAuth authenticator for Intercom API.

    Intercom does not support refresh tokens, so this authenticator
    simply returns the existing access_token from config without
    making any HTTP requests to an auth endpoint.
    """

    @property
    def oauth_request_body(self) -> dict:
        return {}

    def update_access_token_locally(self) -> None:
        """Set access_token directly from config - no HTTP refresh needed."""
        self.access_token = self.config["access_token"]
        self.last_refreshed = utc_now()
        self.expires_in = 7 * 24 * 3600
        self._tap._config["access_token"] = self.access_token
        self._tap._config["expires_in"] = self.expires_in

        if self._tap.config_file is not None:
            with open(self._tap.config_file, "r") as infile:
                file_config = json.load(infile)
            file_config["access_token"] = self.access_token
            file_config["expires_in"] = self.expires_in
            with open(self._tap.config_file, "w") as outfile:
                json.dump(file_config, outfile, indent=4)
