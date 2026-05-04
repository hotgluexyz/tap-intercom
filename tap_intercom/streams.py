"""
This module defines the stream classes and their individual sync logic.
"""


import datetime
import csv
import gzip
import hashlib
import io
import re
import time
import zipfile
import pytz
from typing import Iterator, List

import singer
from singer import Transformer, metrics, metadata, UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING
from singer.transform import transform, unix_milliseconds_to_datetime
from dateutil.parser import parse
from requests.exceptions import ConnectionError, Timeout

from tap_intercom.client import (
    API_VERSION,
    IntercomBadRequestError,
    IntercomClient,
    IntercomError,
    raise_for_error,
)
from tap_intercom.transform import (transform_json, transform_times, find_datetimes_in_schema)

import concurrent.futures

LOGGER = singer.get_logger()

MAX_PAGE_SIZE = 150


class BaseStream:
    """
    A base class representing singer streams.

    :param client: The API client used to extract records from external source
    """
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    to_replicate = True
    path = None
    params = {}
    parent = None
    data_key = None
    child = None

    def __init__(self, client: IntercomClient, catalog, selected_streams):
        self.client = client
        self.catalog = catalog
        self.selected_streams = selected_streams

    def get_records(self, bookmark_datetime: datetime = None, is_parent: bool = False, stream_metadata=None, end_date=None) -> list:
        """
        Returns a list of records for that stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :param is_parent: If true, may change the type of data
            that is returned for a child stream to consume
        :param stream_metadata: Stream metadata dict, if required by the child get_records()
            method.
        :return: list of records
        """
        raise NotImplementedError("Child classes of BaseStream require "
                                  "`get_records` implementation")

    def generate_record_hash(self, original_record):
        """
            Function to generate the hash of name, full_name and label to use it as a Primary Key
        """
        # There are 2 types for data_attributes in Intercom
        # -> Default: As discussed with support, there is an 'id' for custom data_attributes and that will be unique
        # -> Custom: Used 'name' and 'description' for identifying the data uniquely
        fields_to_hash = ['id', 'name', 'description']
        hash_string = ''

        for key in fields_to_hash:
            hash_string += str(original_record.get(key, ''))

        hash_string_bytes = hash_string.encode('utf-8')
        hashed_string = hashlib.sha256(hash_string_bytes).hexdigest()

        # Add Primary Key hash in the record
        original_record['_sdc_record_hash'] = hashed_string
        return original_record

    @staticmethod
    def epoch_milliseconds_to_dt_str(timestamp: float) -> str:
        # Convert epoch milliseconds to datetime object in UTC format
        new_dttm = unix_milliseconds_to_datetime(timestamp)
        return new_dttm

    @staticmethod
    def dt_to_epoch_seconds(dt_object: datetime) -> float:
        return datetime.datetime.timestamp(dt_object)

    def sync_substream(self, parent_id, stream_schema, stream_metadata, parent_replication_key, state, is_last=None):
        """
            Sync sub-stream data based on parent id and update the state to parent's replication value
        """
        schema_datetimes = find_datetimes_in_schema(stream_schema)
        LOGGER.info("Syncing: {}, parent_stream: {}, parent_id: {}".format(self.tap_stream_id, self.parent.tap_stream_id, parent_id))
        call_path = self.path.format(parent_id)
        response = self.client.get(call_path, params=self.params)

        data_for_transform = {self.data_key: [response]}

        transformed_records = transform_json(data_for_transform, self.tap_stream_id, self.data_key)
        LOGGER.info("Synced: {}, parent_id: {}, records: {}".format(self.tap_stream_id, parent_id, len(transformed_records)))
        with metrics.record_counter(self.tap_stream_id) as counter:
            # Iterate over conversation_parts records
            for record in transformed_records:
                transform_times(record, schema_datetimes) # Transfrom datetimes fields of record

                transformed_record = transform(record,
                                                stream_schema,
                                                integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                metadata=stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record, time_extracted=singer.utils.now())
                counter.increment()

            LOGGER.info("FINISHED Syncing: {}, total_records: {}.".format(self.tap_stream_id, counter.value))

        # Conversations(parent) are coming in ascending order
        # so write state with updated_at of conversation after yielding conversation_parts for it.
        parent_bookmark_value = self.epoch_milliseconds_to_dt_str(parent_replication_key)
        state = singer.write_bookmark(state,
                                        self.tap_stream_id,
                                        self.replication_key,
                                        parent_bookmark_value)
        singer.write_state(state)

        return state

# pylint: disable=abstract-method
class IncrementalStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    INCREMENTAL replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'INCREMENTAL'
    to_write_intermediate_bookmark = False

    def mark_last_item(self, generator):
        iterator = iter(generator)
        try:
            # Attempt to get the first item
            previous = next(iterator)
        except StopIteration:
            # If generator is empty, exit the function
            return
        
        # Iterate through remaining items
        for current in iterator:
            yield previous, False
            previous = current
        
        # Yield the last item with `is_last=True`
        yield previous, True

    # Disabled `unused-argument` as it causing pylint error.
    # Method which call this `sync` method is passing unused argument.So, removing argument would not work.
    # pylint: disable=too-many-arguments,unused-argument
    def sync(self,
             state: dict,
             stream_schema: dict,
             stream_metadata: dict,
             config: dict,
             transformer: Transformer) -> dict:
        """
        The sync logic for an incremental stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :return: State data in the form of a dictionary
        """

        # Check if the current stream has child stream or not
        has_child = self.child is not None and len(self.child) > 0
        # Child stream class
        child_streams: List[BaseStream] = [STREAMS.get(child) for child in self.child] if has_child else []

        # Get current stream bookmark
        parent_bookmark = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        parent_bookmark_utc = singer.utils.strptime_to_utc(parent_bookmark)
        sync_start_date = parent_bookmark_utc

        # get end_date
        end_date = None
        end_date_timestamp = None
        if config.get('end_date'):
            end_date = singer.utils.strptime_to_utc(config.get('end_date'))
            end_date_timestamp = end_date.timestamp() * 1000

        is_parent_selected = True
        is_child_selected = False

        # If the current stream has a child stream, then get the child stream's bookmark
        # And update the sync start date to minimum of parent bookmark or child bookmark
        if has_child:
            child_streams_auxiliary_data = {}
            for child_stream in child_streams:
                child_bookmark = singer.get_bookmark(state, child_stream.tap_stream_id, self.replication_key, config['start_date'])
                child_bookmark_utc = singer.utils.strptime_to_utc(child_bookmark)
                child_bookmark_ts = child_bookmark_utc.timestamp() * 1000

                is_parent_selected = self.tap_stream_id in self.selected_streams
                is_child_selected = child_stream.tap_stream_id in self.selected_streams

                if is_parent_selected and is_child_selected:
                    sync_start_date = min(parent_bookmark_utc, child_bookmark_utc)
                elif is_parent_selected:
                    sync_start_date = parent_bookmark_utc
                elif is_child_selected:
                    sync_start_date = singer.utils.strptime_to_utc(child_bookmark)

                # Create child stream object and generate schema
                child_stream_obj = child_stream(self.client, self.catalog, self.selected_streams)
                child_stream_ = self.catalog.get_stream(child_stream.tap_stream_id)
                child_schema = child_stream_.schema.to_dict()
                child_metadata = metadata.to_map(child_stream_.metadata)
                if is_child_selected:
                    # Write schema for child stream as it will be synced by the parent stream
                    singer.write_schema(
                        child_stream.tap_stream_id,
                        child_schema,
                        child_stream.key_properties,
                        child_stream.replication_key
                    )
                child_streams_auxiliary_data[child_stream.tap_stream_id] = {
                    "is_selected": is_child_selected,
                    "bookmark": child_bookmark_ts,
                    "schema": child_schema,
                    "metadata": child_metadata,
                    "obj": child_stream_obj
                }

        LOGGER.info("Stream: {}, initial max_bookmark_value: {}".format(self.tap_stream_id, sync_start_date))
        max_datetime = sync_start_date
        # We are not using singer's record counter as the counter reset after 60 seconds
        record_counter = 0

        schema_datetimes = find_datetimes_in_schema(stream_schema)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record, is_last in self.mark_last_item(self.get_records(sync_start_date, stream_metadata=stream_metadata, end_date=end_date)):
                transform_times(record, schema_datetimes)

                record_datetime = singer.utils.strptime_to_utc(
                    self.epoch_milliseconds_to_dt_str(
                        record[self.replication_key])
                    )

                # Write the record if the parent is selected
                if is_parent_selected and record_datetime >= parent_bookmark_utc and (record_datetime <= end_date if end_date else True):
                    record_counter += 1
                    transformed_record = transform(record,
                                                    stream_schema,
                                                    integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                    metadata=stream_metadata)
                    # Write record if a parent is selected
                    singer.write_record(self.tap_stream_id, transformed_record, time_extracted=singer.utils.now())
                    counter.increment()
                    max_datetime = max(record_datetime, max_datetime)

                if self.to_write_intermediate_bookmark and record_counter == MAX_PAGE_SIZE:
                    # Write bookmark and state after every page of records
                    state = singer.write_bookmark(state,
                                      self.tap_stream_id,
                                      self.replication_key,
                                      singer.utils.strftime(max_datetime))
                    singer.write_state(state)
                    # Reset counter
                    record_counter = 0

                # Sync child stream, if the child is selected and if we have records greater than the child stream bookmark
                if has_child:
                    for child_stream_auxiliary_data in child_streams_auxiliary_data.values():
                        child_stream_obj = child_stream_auxiliary_data.get("obj")
                        child_schema = child_stream_auxiliary_data.get("schema")
                        child_metadata = child_stream_auxiliary_data.get("metadata")
                        child_bookmark_ts = child_stream_auxiliary_data.get("bookmark")
                        is_child_selected = child_stream_auxiliary_data.get("is_selected")
                        if is_child_selected and (record[self.replication_key] >= child_bookmark_ts) and (record[self.replication_key] <= end_date_timestamp if end_date else True):
                            state = child_stream_obj.sync_substream(record.get('id'), child_schema, child_metadata, child_bookmark_ts, state, is_last)

            bookmark_date = singer.utils.strftime(max_datetime)
            LOGGER.info("FINISHED Syncing: {}, total_records: {}.".format(self.tap_stream_id, counter.value))

        LOGGER.info("Stream: {}, writing final bookmark".format(self.tap_stream_id))
        state = singer.write_bookmark(state,
                                      self.tap_stream_id,
                                      self.replication_key,
                                      bookmark_date)
        return state


# pylint: disable=abstract-method
class FullTableStream(BaseStream):
    """
    A child class of a base stream used to represent streams that use the
    FULL_TABLE replication method.

    :param client: The API client used extract records from the external source
    """
    replication_method = 'FULL_TABLE'
    # Boolean flag to do sync with activate version for Company and Contact Attributes streams
    sync_with_version = False

    # Disabled `unused-argument` as it causing pylint error.
    # Method which call this `sync` method is passing unused argument. So, removing argument would not work.
    # pylint: disable=too-many-arguments,unused-argument
    def sync(self,
             state: dict,
             stream_schema: dict,
             stream_metadata: dict,
             config: dict,
             transformer: Transformer) -> dict:
        """
        The sync logic for an full table stream.

        :param state: A dictionary representing singer state
        :param stream_schema: A dictionary containing the stream schema
        :param stream_metadata: A dictionnary containing stream metadata
        :param config: A dictionary containing tap config data
        :return: State data in the form of a dictionary
        """
        schema_datetimes = find_datetimes_in_schema(stream_schema)
        if self.sync_with_version:
            # Write activate version message
            activate_version = int(time.time() * 1000)
            activate_version_message = singer.ActivateVersionMessage(
                stream=self.tap_stream_id,
                version=activate_version)
            singer.write_message(activate_version_message)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():

                # For company and contact attributes, it is difficult to define a Primary Key
                # Thus we create a hash of some records and use it as the PK
                if self.tap_stream_id in ['company_attributes', 'contact_attributes']:
                    record = self.generate_record_hash(record)

                transform_times(record, schema_datetimes)

                transformed_record = transform(record,
                                                stream_schema,
                                                integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                metadata=stream_metadata)
                # Write records with time_extracted field
                if self.sync_with_version:
                    # Using "write_message" if the version is found. As "write_record" params do not contain "version"
                    singer.write_message(singer.RecordMessage(stream=self.tap_stream_id, record=transformed_record, version=activate_version, time_extracted=singer.utils.now()))
                else:
                    singer.write_record(self.tap_stream_id, transformed_record, time_extracted=singer.utils.now())
                counter.increment()

            LOGGER.info("FINISHED Syncing: {}, total_records: {}.".format(self.tap_stream_id, counter.value))

        # Write activate version after syncing
        if self.sync_with_version:
            singer.write_message(activate_version_message)

        return state


class AdminList(FullTableStream):
    """
    This stream is not replicated and only used by the Admins stream.

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-admins
    """
    tap_stream_id = 'admin_list'
    key_properties = ['id']
    to_replicate = False
    path = 'admins'
    data_key = 'admins'

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        response = self.client.get(self.path)

        if not response.get(self.data_key):
            LOGGER.critical('response is empty for {} stream'.format(self.tap_stream_id))
            raise IntercomError

        # Only yield records when called by child streams
        if is_parent:
            for record in response.get(self.data_key):
                yield record.get('id')

class Admins(FullTableStream):
    """
    Retrieves admins for a workspace

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#view-an-admin
    """
    tap_stream_id = 'admins'
    key_properties = ['id']
    path = 'admins/{}'
    parent = AdminList

    def get_parent_data(self, bookmark_datetime: datetime = None) -> list:
        """
        Returns a list of records from the parent stream.

        :param bookmark_datetime: The datetime object representing the
            bookmark date
        :return: A list of records
        """
        # pylint: disable=not-callable
        parent = self.parent(self.client, self.catalog, self.selected_streams)
        return parent.get_records(bookmark_datetime, is_parent=True)

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))
        admins = []

        for record in self.get_parent_data():
            call_path = self.path.format(record)
            results = self.client.get(call_path)

            admins.append(results)

        yield from admins


class Companies(IncrementalStream):
    """
    Retrieves companies data using the Scroll API

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#iterating-over-all-companies
    """
    tap_stream_id = 'companies'
    key_properties = ['id']
    path = 'companies/scroll' # using Scroll API
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    data_key = 'data'

    def get_records(self, bookmark_datetime=None, is_parent=False, stream_metadata=None, end_date=None) -> Iterator[list]:
        scrolling = True
        params = {}
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while scrolling:
            response = self.client.get(self.path, params=params)

            if response.get(self.data_key) is None:
                LOGGER.warning('response is empty for "{}" stream'.format(self.tap_stream_id))

            records = transform_json(response, self.tap_stream_id, self.data_key)
            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(records)))

            # stop scrolling if 'data' array is empty
            if 'scroll_param' in response and response.get(self.data_key):
                scroll_param = response.get('scroll_param')
                params = {'scroll_param': scroll_param}
                LOGGER.info("Syncing next page")
            else:
                scrolling = False

            yield from records


class CompanyAttributes(FullTableStream):
    """
    Retrieves company attributes

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-data-attributes
    """
    tap_stream_id = 'company_attributes'
    key_properties = ['_sdc_record_hash']
    path = 'data_attributes'
    params = {'model': 'company'}
    data_key = 'data'
    # Sync with activate version
    # As we are preparing the hash of ['id', 'name', 'description'] and using it as the Primary Key and there are chances
    # of field value being updated, thus, on the target side, there will be a redundant entry of the same record.
    sync_with_version = True

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        next_page = None
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key,  [])


class CompanySegments(IncrementalStream):
    """
    Retrieve company segments

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-segments
    """
    tap_stream_id = 'company_segments'
    key_properties = ['id']
    path = 'segments'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {
        'type': 'company',
        'include_count': 'true'
        }
    data_key = 'segments'

    def get_records(self, bookmark_datetime=None, is_parent=False, stream_metadata=None, end_date=None) -> Iterator[list]:
        paging = True
        next_page = None
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key)


class Conversations(IncrementalStream):
    """
    Retrieve conversations

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#search-for-conversations
    """
    tap_stream_id = 'conversations'
    key_properties = ['id']
    path = 'conversations/search'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'display_as': 'plaintext'}
    data_key = 'conversations'
    per_page = MAX_PAGE_SIZE
    child = ['conversation_parts', 'conversation_details']

    def get_records(self, bookmark_datetime=None, is_parent=False, stream_metadata=None, end_date=None) -> Iterator[list]:
        paging = True
        starting_after = None
        date_filter = {
            'operator': 'OR',
            'value': [
                {
                    'field': self.replication_key,
                    'operator': '>',
                    'value': self.dt_to_epoch_seconds(bookmark_datetime)
                },
                {
                    'field': self.replication_key,
                    'operator': '=',
                    'value': self.dt_to_epoch_seconds(bookmark_datetime)
                }
            ]
        }
        if end_date:
            date_filter = {
                'operator': 'AND',
                'value': [
                    date_filter,
                    {
                        'field': self.replication_key,
                        'operator': '<',
                        'value': self.dt_to_epoch_seconds(end_date) + 1
                    }
                ]
            }
            
        search_query = {
            'pagination': {
                'per_page': self.per_page
            },
            'query': date_filter,
            "sort": {
                "field": self.replication_key,
                "order": "ascending"
            }
        }
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.post(self.path, json=search_query)

            if 'pages' in response and response.get('pages', {}).get('next'):
                starting_after = response.get('pages').get('next').get('starting_after')
                search_query['pagination'].update({'starting_after': starting_after})
            else:
                paging = False

            records = transform_json(response, self.tap_stream_id, self.data_key)
            LOGGER.info("Synced: {} for page: {}, records: {}".format(self.tap_stream_id, response.get('pages', {}).get('page'), len(records)))

            if is_parent:
                for record in records:
                    yield record.get('id')
            else:
                yield from records

class ConversationDetails(BaseStream):
    """
    Retrieve conversation details
    Docs: https://developers.intercom.com/docs/references/rest-api/api.intercom.io/conversations
    """
    tap_stream_id = 'conversation_details'
    key_properties = ['id']
    path = 'conversations/{}'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    parent = Conversations
    params = {'display_as': 'plaintext'}
    data_key = 'conversations'
    max_concurrency = 10
    record_count = 0
    conversation_ids = []

    def get_conversation_parts(self, conversation_id):
        LOGGER.info("Syncing: {}, parent_stream: {}, parent_id: {}".format(self.tap_stream_id, self.parent.tap_stream_id, conversation_id))
        call_path = self.path.format(conversation_id)
        response = self.client.get(call_path, params=self.params)
        return response

    def sync_substream(self, parent_id, stream_schema, stream_metadata, parent_replication_key, state, is_last):
        """
            Sync sub-stream data based on parent id and update the state to parent's replication value
        """

        if len(self.conversation_ids) < self.max_concurrency:
            self.conversation_ids.append(parent_id)
            self.record_count += 1
            
        if len(self.conversation_ids) < self.max_concurrency and not is_last:
            return state
        else:
            schema_datetimes = find_datetimes_in_schema(stream_schema)
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_concurrency
            ) as executor:
                futures = {
                    executor.submit(self.get_conversation_parts, x): x for x in self.conversation_ids
                }

            # Process each future as it completes
            for future in concurrent.futures.as_completed(futures):
                response = future.result()
                data_for_transform = {self.data_key: [response]}
                parent = datetime.datetime.utcfromtimestamp(parent_replication_key/1000)
                state_value = parent or datetime.datetime(2010, 1, 1)

                state_value = state_value.replace(tzinfo=pytz.UTC)
                bookmark_state = state_value

                transformed_records = transform_json(data_for_transform, self.tap_stream_id, self.data_key)
                LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(transformed_records)))
                with metrics.record_counter(self.tap_stream_id) as counter:
                    # Iterate over conversation_parts records
                    for record in transformed_records:
                        transform_times(record, schema_datetimes) # Transfrom datetimes fields of record

                        transformed_record = transform(record,
                                                        stream_schema,
                                                        integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                        metadata=stream_metadata)
                        created_at = parse(transformed_record["created_at"])
                        if state_value and created_at > state_value:
                            if bookmark_state < created_at:
                                bookmark_state = created_at
                            singer.write_record(self.tap_stream_id, transformed_record, time_extracted=singer.utils.now())
                            counter.increment()
                        else:
                            pass

                LOGGER.info("FINISHED Syncing: {}, total_records: {}.".format(self.tap_stream_id, counter.value))

            # restart conversation_ids
            self.conversation_ids = []
            # Conversations(parent) are coming in ascending order
            # so write state with updated_at of conversation after yielding conversation_parts for it.
            # parent_bookmark_value = self.epoch_milliseconds_to_dt_str(parent_replication_key)
            state = singer.write_bookmark(state,
                                            self.tap_stream_id,
                                            self.replication_key,
                                            bookmark_state.isoformat())
            singer.write_state(state)

            return state

class ConversationParts(BaseStream):
    """
    Retrieve conversation parts

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#retrieve-a-conversation
    """
    tap_stream_id = 'conversation_parts'
    key_properties = ['id']
    path = 'conversations/{}'
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    parent = Conversations
    params = {'display_as': 'plaintext'}
    data_key = 'conversations'
    max_concurrency = 10
    record_count = 0
    conversation_ids = []

    def get_conversation_parts(self, conversation_id):
        LOGGER.info("Syncing: {}, parent_stream: {}, parent_id: {}".format(self.tap_stream_id, self.parent.tap_stream_id, conversation_id))
        call_path = self.path.format(conversation_id)
        response = self.client.get(call_path, params=self.params)
        return response

    def sync_substream(self, parent_id, stream_schema, stream_metadata, parent_replication_key, state, is_last):
        """
            Sync sub-stream data based on parent id and update the state to parent's replication value
        """

        if len(self.conversation_ids) < self.max_concurrency:
            self.conversation_ids.append(parent_id)
            self.record_count += 1
            
        if len(self.conversation_ids) < self.max_concurrency and not is_last:
            return state
        else:
            schema_datetimes = find_datetimes_in_schema(stream_schema)
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_concurrency
            ) as executor:
                futures = {
                    executor.submit(self.get_conversation_parts, x): x for x in self.conversation_ids
                }

            # Process each future as it completes
            for future in concurrent.futures.as_completed(futures):
                response = future.result()
                data_for_transform = {self.data_key: [response]}
                parent = datetime.datetime.utcfromtimestamp(parent_replication_key/1000)
                state_value = parent or datetime.datetime(2010, 1, 1)

                state_value = state_value.replace(tzinfo=pytz.UTC)
                bookmark_state = state_value

                transformed_records = transform_json(data_for_transform, self.tap_stream_id, self.data_key)
                LOGGER.info("Synced: {}, parent_id: {}, records: {}".format(self.tap_stream_id, parent_id, len(transformed_records)))
                with metrics.record_counter(self.tap_stream_id) as counter:
                    # Iterate over conversation_parts records
                    for record in transformed_records:
                        transform_times(record, schema_datetimes) # Transfrom datetimes fields of record

                        transformed_record = transform(record,
                                                        stream_schema,
                                                        integer_datetime_fmt=UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                                                        metadata=stream_metadata)
                        created_at = parse(transformed_record["created_at"])
                        if state_value and created_at > state_value:
                            if bookmark_state < created_at:
                                bookmark_state = created_at
                            singer.write_record(self.tap_stream_id, transformed_record, time_extracted=singer.utils.now())
                            counter.increment()
                        else:
                            pass

                LOGGER.info("FINISHED Syncing: {}, total_records: {}.".format(self.tap_stream_id, counter.value))

            # restart conversation_ids
            self.conversation_ids = []
            # Conversations(parent) are coming in ascending order
            # so write state with updated_at of conversation after yielding conversation_parts for it.
            # parent_bookmark_value = self.epoch_milliseconds_to_dt_str(parent_replication_key)
            state = singer.write_bookmark(state,
                                            self.tap_stream_id,
                                            self.replication_key,
                                            bookmark_state.isoformat())
            singer.write_state(state)

            return state


class ContactAttributes(FullTableStream):
    """
    Retrieve contact attributes

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-data-attributes
    """
    tap_stream_id = 'contact_attributes'
    key_properties = ['_sdc_record_hash']
    path = 'data_attributes'
    params = {'model': 'contact'}
    data_key = 'data'
    # Sync with activate version
    # As we are preparing the hash of ['id', 'name', 'description'] and using it as the Primary Key and there are chances
    # of field value being updated, thus, on the target side, there will be a redundant entry of the same record.
    sync_with_version = True

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))
        paging = True
        next_page = None

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key,  [])


class Contacts(IncrementalStream):
    """
    Retrieve contacts

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#search-for-contacts
    """
    tap_stream_id = 'contacts'
    key_properties = ['id']
    path = 'contacts/search'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    data_key = 'data'
    per_page = MAX_PAGE_SIZE
    # addressable_list_fields = ['tags', 'notes', 'companies']
    addressable_list_fields = ['tags', 'companies']
    to_write_intermediate_bookmark = True
    stream_start_time = int(time.time())

    def get_addressable_list(self, contact_list: dict, stream_metadata: dict) -> dict:
        params = {
            'display_as': 'plaintext',
            'per_page': 60 # addressable_list endpoints have a different max page size in Intercom's API v2.0
        }

        for record in contact_list.get(self.data_key):
            for addressable_list_field in self.addressable_list_fields:
                # List of values from the API
                values = []
                data = record.get(addressable_list_field)
                paging = True
                next_page = None
                endpoint = data.get('url')

                # Do not do the API call to get addressable fields:
                #   If the field is not selected
                #   If we have 0 records
                #   If we have less than 10 records ie. the 'has_more' field is 'False'
                if not stream_metadata.get(('properties', addressable_list_field), {}).get('selected') or \
                    not data.get('total_count') > 0 or \
                        not data.get('has_more'):
                    continue

                while paging:
                    response = self.client.get(endpoint, url=next_page, params=params)

                    if 'pages' in response and response.get('pages', {}).get('next'):
                        next_page = response.get('pages', {}).get('next')
                        endpoint = None
                    else:
                        paging = False

                    values.extend(response.get(self.data_key, []))
                record[addressable_list_field][self.data_key] = values

        return contact_list

    def get_records(self, bookmark_datetime=None, is_parent=False, stream_metadata=None, end_date=None) -> Iterator[list]:
        paging = True
        starting_after = None
        date_filter = {
            'operator': 'OR',
            'value': [
                {
                    'field': self.replication_key,
                    'operator': '>',
                    'value': self.dt_to_epoch_seconds(bookmark_datetime)
                },
                {
                    'field': self.replication_key,
                    'operator': '=',
                    'value': self.dt_to_epoch_seconds(bookmark_datetime)
                }
            ]
        }

        if end_date:
            date_filter = {
                'operator': 'AND',
                'value': [
                    date_filter,
                    {
                        'field': self.replication_key,
                        'operator': '<',
                        'value': self.dt_to_epoch_seconds(end_date) + 1
                    }
                ]
            }
        else:
            date_filter = {
                'operator': 'AND',
                'value': [
                    date_filter,
                    {
                        'field': self.replication_key,
                        'operator': '<',
                        'value': self.stream_start_time + 1
                    }
                ]
            }

        search_query = {
            'pagination': {
                'per_page': self.per_page
            },
            'query': date_filter,
            'sort': {
                'field': self.replication_key,
                'order': 'ascending'
                }
        }
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.post(self.path, json=search_query)

            if 'pages' in response and response.get('pages', {}).get('next'):
                starting_after = response.get('pages').get('next').get('starting_after')
                search_query['pagination'].update({'starting_after': starting_after})
            else:
                paging = False

            # Check each contact for any records in each addressable-list object (tags, notes, companies)
            response = self.get_addressable_list(response, stream_metadata=stream_metadata)

            records = transform_json(response, self.tap_stream_id, self.data_key)
            LOGGER.info("Synced: {} for page: {}, records: {}".format(self.tap_stream_id, response.get('pages', {}).get('page'), len(records)))

            yield from records


class DataExport(BaseStream):
    """
    Export outbound content engagement data via Intercom's async export API.
    """
    tap_stream_id = 'data_export'
    replication_method = 'INCREMENTAL'
    replication_key = 'created_at'
    valid_replication_keys = ['created_at']
    path = 'export/content/data'

    MAX_WINDOW_DAYS = 90
    POLL_MAX_ATTEMPTS = 60
    POLL_SLEEP_SECONDS = 1
    DOWNLOAD_MAX_ATTEMPTS = 5
    DOWNLOAD_SLEEP_SECONDS = 2
    STREAM_PREFIX = "data_export_"
    DEFAULT_REPLICATION_KEY = "created_at"

    def sync(self,
             state: dict,
             stream_schema: dict,
             stream_metadata: dict,
             config: dict,
             transformer: Transformer) -> dict:
        """
        Run async export jobs in <=90 day windows and emit dynamic streams per CSV prefix.
        """
        start_dt = self._get_created_after_from_state(state, config['start_date'])
        end_dt = singer.utils.strptime_to_utc(config.get('end_date')) if config.get('end_date') else singer.utils.now()

        if start_dt >= end_dt:
            LOGGER.info("Skipping data_export: start is not lower than end.")
            return state

        max_parent_bookmark = start_dt
        stream_schema_cache = {}

        window_start = start_dt
        while window_start < end_dt:
            window_end = min(window_start + datetime.timedelta(days=self.MAX_WINDOW_DAYS), end_dt)
            export_job = self._create_export_job(window_start, window_end)
            export_job = self._poll_export_job(export_job.get("job_identifier"))

            if export_job.get("status") == "no_data":
                max_parent_bookmark = max(max_parent_bookmark, window_end)
                window_start = window_end
                continue

            payload = self._download_export_payload(export_job)
            max_parent_bookmark = max(max_parent_bookmark, window_end)

            for source_file, row in self._iter_export_rows(payload):
                prefix = self._derive_prefix(source_file)
                stream_id = "{}{}".format(self.STREAM_PREFIX, prefix)
                replication_key = self.DEFAULT_REPLICATION_KEY

                stream_schema_obj = stream_schema_cache.get(stream_id)
                if stream_schema_obj is None:
                    candidate_schema = self._build_dynamic_schema(row)
                    stream_schema_cache[stream_id] = candidate_schema
                    singer.write_schema(stream_id, candidate_schema, [], replication_key)

                singer.write_record(stream_id, row, time_extracted=singer.utils.now())

            window_start = window_end

        state = singer.write_bookmark(
            state,
            self.tap_stream_id,
            self.replication_key,
            singer.utils.strftime(max_parent_bookmark)
        )

        return state

    def _get_created_after_from_state(self, state, configured_start_date):
        configured_start = singer.utils.strptime_to_utc(configured_start_date)
        data_export_bookmark = state.get("bookmarks", {}).get(self.tap_stream_id, {})
        if isinstance(data_export_bookmark, dict):
            bookmark_value = data_export_bookmark.get(self.replication_key)
        else:
            bookmark_value = data_export_bookmark

        bookmark_dt = self._parse_bookmark_value(bookmark_value)
        return bookmark_dt if bookmark_dt else configured_start

    @staticmethod
    def _parse_bookmark_value(value):
        try:
            return singer.utils.strptime_to_utc(value)
        except Exception:
            return None

    def _create_export_job(self, created_after_dt, created_before_dt):
        created_after = int(created_after_dt.timestamp())
        created_before = int(created_before_dt.timestamp())
        payload = {
            "created_at_after": created_after,
            "created_at_before": created_before
        }

        try:
            response = self.client.post(self.path, json=payload)
        except IntercomBadRequestError as exc:
            error_message = str(exc)
            if "export period is longer than 90 days" in error_message:
                raise IntercomError(
                    "Invalid Data Export window: created_at_after={} created_at_before={} exceeds 90 days.".format(
                        created_after, created_before
                    )
                ) from None
            raise

        if not response.get("job_identifier"):
            raise IntercomError("Data export create job response missing job_identifier.")

        return response

    def _poll_export_job(self, job_identifier):
        if not job_identifier:
            raise IntercomError("Cannot poll data export status without job_identifier.")

        status_path = "{}/{}".format(self.path, job_identifier)
        for _ in range(self.POLL_MAX_ATTEMPTS):
            response = self.client.get(status_path)
            status = response.get("status")

            if status in {"completed", "no_data"}:
                return response
            if status in {"failed", "canceled"}:
                raise IntercomError("Data export job {} finished with status '{}'.".format(job_identifier, status))
            if status not in {"pending", "in_progress"}:
                raise IntercomError("Data export job {} returned unknown status '{}'.".format(job_identifier, status))

            time.sleep(self.POLL_SLEEP_SECONDS)

        raise IntercomError("Data export job {} polling timed out.".format(job_identifier))

    def _download_export_payload(self, export_job):
        job_identifier = export_job.get("job_identifier")
        download_url = export_job.get("download_url")
        if not download_url:
            download_url = "{}/download/content/data/{}".format(self.client.base_url, job_identifier)

        if not download_url.startswith("http"):
            download_url = "{}/{}".format(self.client.base_url.rstrip("/"), download_url.lstrip("/"))

        session = getattr(self.client, "_IntercomClient__session")
        access_token = getattr(self.client, "_IntercomClient__access_token")
        request_timeout = getattr(self.client, "_IntercomClient__request_timeout")

        headers = {
            "Authorization": "Bearer {}".format(access_token),
            "Accept": "application/octet-stream",
            "Intercom-Version": API_VERSION
        }

        for attempt in range(1, self.DOWNLOAD_MAX_ATTEMPTS + 1):
            try:
                response = session.get(download_url, headers=headers, timeout=request_timeout)
            except (Timeout, ConnectionError):
                if attempt < self.DOWNLOAD_MAX_ATTEMPTS:
                    sleep_seconds = min(self.DOWNLOAD_SLEEP_SECONDS ** attempt, 60)
                    time.sleep(sleep_seconds)
                    continue
                raise

            if response.status_code == 200:
                return response.content

            if attempt < self.DOWNLOAD_MAX_ATTEMPTS and (response.status_code == 429 or response.status_code >= 500):
                sleep_seconds = min(self.DOWNLOAD_SLEEP_SECONDS ** attempt, 60)
                time.sleep(sleep_seconds)
                continue

            raise_for_error(response)

        raise IntercomError("Data export download failed after retries.")

    def _iter_export_rows(self, payload):
        payload_buffer = io.BytesIO(payload)

        if zipfile.is_zipfile(payload_buffer):
            payload_buffer.seek(0)
            with zipfile.ZipFile(payload_buffer) as zip_archive:
                for filename in sorted(zip_archive.namelist()):
                    if not filename.lower().endswith(".csv"):
                        continue
                    with zip_archive.open(filename) as file_obj:
                        for row in self._read_csv_rows(file_obj):
                            yield filename, row
            return

        payload_buffer.seek(0)
        with gzip.GzipFile(fileobj=payload_buffer, mode="rb") as gz_file:
            for row in self._read_csv_rows(gz_file):
                yield "data_export.csv", row

    @staticmethod
    def _read_csv_rows(binary_file):
        text_stream = io.TextIOWrapper(binary_file, encoding="utf-8")
        csv_reader = csv.DictReader(text_stream)
        for row in csv_reader:
            yield row

    @staticmethod
    def _derive_prefix(source_file):
        filename = source_file.split("/")[-1]
        name_without_ext = filename.rsplit(".", 1)[0]
        parts = name_without_ext.split("_")
        raw_prefix = "_".join(parts[:-1]) if len(parts) > 1 else name_without_ext
        normalized = re.sub(r"[^a-z0-9_]+", "_", raw_prefix.lower())
        normalized = re.sub(r"_+", "_", normalized).strip("_")
        return normalized or "unknown"

    @staticmethod
    def _build_dynamic_schema(record):
        properties = {}
        for key in record.keys():
            properties[key] = {"type": ["null", "string", "integer", "number", "boolean"]}

        return {
            "type": "object",
            "additionalProperties": False,
            "properties": properties
        }

class Segments(IncrementalStream):
    """
    Retrieve segments

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-segments
    """
    tap_stream_id = 'segments'
    key_properties = ['id']
    path = 'segments'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'include_count': 'true'}
    data_key = 'segments'

    def get_records(self, bookmark_datetime=None, is_parent=False, stream_metadata=None, end_date=None) -> Iterator[list]:
        paging = True
        next_page = None
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key,  [])


class Tags(FullTableStream):
    """
    Retrieve tags

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-tags-for-an-app
    """
    tap_stream_id = 'tags'
    key_properties =['id']
    path = 'tags'
    data_key = 'data'

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        next_page = None
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key,  [])


class Teams(FullTableStream):
    """
    Retrieve teams

    Docs: https://developers.intercom.com/intercom-api-reference/v2.0/reference#list-teams
    """
    tap_stream_id = 'teams'
    key_properties = ['id']
    path = 'teams'
    data_key = 'teams'

    def get_records(self, bookmark_datetime=None, is_parent=False) -> Iterator[list]:
        paging = True
        next_page = None
        LOGGER.info("Syncing: {}".format(self.tap_stream_id))

        while paging:
            response = self.client.get(self.path, url=next_page, params=self.params)

            LOGGER.info("Synced: {}, records: {}".format(self.tap_stream_id, len(response.get(self.data_key, []))))
            if 'pages' in response and response.get('pages', {}).get('next'):
                next_page = response.get('pages', {}).get('next')
                self.path = None
                LOGGER.info("Syncing next page")
            else:
                paging = False

            yield from response.get(self.data_key,  [])


STREAMS = {
    "admin_list": AdminList,
    "admins": Admins,
    "companies": Companies,
    "company_attributes": CompanyAttributes,
    "company_segments": CompanySegments,
    "conversations": Conversations,
    "conversation_details": ConversationDetails,
    "conversation_parts": ConversationParts,
    "contact_attributes": ContactAttributes,
    "contacts": Contacts,
    "data_export": DataExport,
    "segments": Segments,
    "tags": Tags,
    "teams": Teams
}
