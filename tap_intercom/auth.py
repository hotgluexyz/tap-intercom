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
        pass # Not needed for Intercom as we are using access_token directly

    def update_access_token_locally(self) -> None:
        """Set access_token directly from config - no HTTP refresh needed."""
        self.access_token = self.config["access_token"]
        self.last_refreshed = utc_now()
        self.expires_in = int(utc_now().timestamp()) + 7 * 24 * 3600
        self._tap._config["access_token"] = self.access_token
        self._tap._config["expires_in"] = self.expires_in
        
        # Write the updated config back to the file (only when config was loaded from a path)
        if self._tap.config_file is not None:
            with open(self._tap.config_file, "w") as outfile:
                json.dump(self._tap._config, outfile, indent=4)
