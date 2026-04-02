#!/usr/bin/env python3

import singer

from tap_intercom.discover import discover
from tap_intercom.sync import sync

from hotglue_singer_sdk.tap_base import Tap
from hotglue_singer_sdk.helpers._util import read_json_file
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel
from tap_intercom.auth import IntercomOAuthAuthenticator

LOGGER = singer.get_logger()


class IntercomTap(Tap):
    name = "tap-intercom"

    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property("access_token", th.StringType, required=True),
        th.Property("start_date", th.StringType, required=True),
        th.Property("user_agent", th.StringType, required=True),
        th.Property("client_id", th.StringType),
        th.Property("client_secret", th.StringType),
        th.Property("refresh_token", th.StringType),
        th.Property("request_timeout", th.NumberType),
        th.Property("end_date", th.StringType),
    ).to_dict()

    @classmethod
    def access_token_support(cls, connector=None):
        """Return authenticator class and auth endpoint for token refresh."""
        authenticator = IntercomOAuthAuthenticator
        auth_endpoint = None # Not needed for Intercom as we are using access_token directly
        return authenticator, auth_endpoint

    def discover_streams(self):
        return []

    def run_discovery(self):
        LOGGER.info('Starting discover')
        catalog = discover()
        catalog.dump()
        LOGGER.info('Finished discover')

    def run_sync(self, catalog=None, state=None):
        self.register_streams_from_catalog(catalog)
        self.register_state_from_file(state)

        config = dict(self.config)

        if isinstance(catalog, str):
            catalog_dict = read_json_file(catalog)
        elif isinstance(catalog, dict):
            catalog_dict = catalog
        else:
            catalog_dict = self.input_catalog.to_dict() if self.input_catalog else None

        if catalog_dict:
            from singer.catalog import Catalog
            catalog_obj = Catalog.from_dict(catalog_dict)
        else:
            catalog_obj = discover()

        state_dict = read_json_file(state) if isinstance(state, str) else (state or {})

        sync(config, state_dict, catalog_obj)


def main():
    IntercomTap.cli()


if __name__ == '__main__':
    main()
