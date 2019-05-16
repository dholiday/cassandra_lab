
import os
import uuid
import logging
import json

from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.management import create_keyspace_simple

from cassandra.util import unix_time_from_uuid1


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class BaseTripEventModel(Model):
    """
    Base class so we can define the primary keys of each table separately.
    For this class to make sense, you'll have to define the properties
    `user_id` (columns.UUID)
    `event_time` (columns.TimeUUID)
    `trip_id` (columns.UUID)
    """
    __abstract__ = True
    event_id = columns.UUID(required=True)
    event_type = columns.Text(required=True)
    lat = columns.Float(required=True)
    lng = columns.Float(required=True)
    accuracy = columns.Float(required=True)
    speed = columns.Float(required=True)
    meta = columns.Map(columns.Text, columns.Float)
    sdk = columns.Map(columns.Text, columns.Text)

    # these properties are not persisted; they are used as a transfer mechanism when
    # writing trip events and trips
    score = None
    waypoints = []

    def to_json(self):
        """
        Builds a json-serialized string for this object

        :return: json-encoded string
        """
        return json.dumps(self.get_serializable_dict())

    def get_serializable_dict(self):
        """
        Build a dictionary suitable for serialization from this object. This data structure will match that which
        the client sends to us

        :return: dict representation of this object
        """
        user_dict = {
            "userId": str(self.user_id),
            "eventTime": int(unix_time_from_uuid1(self.event_time)),
            "tripId": str(self.trip_id),
            "eventId": str(self.event_id),
            "eventType": self.event_type,
            "location": {
                "lat": self.lat,
                "lon": self.lng,
                "accuracy": self.accuracy
            },
            "speed": self.speed
        }
        if self.sdk != {}:
            user_dict.update({"sdk": self.sdk})
        if self.meta != None:
            user_dict.update(self.meta)

        return user_dict


class TripEventByUser(BaseTripEventModel):
    """
    ORM class for retrieving and saving trip events
    """
    __table_name__ = "trip_events_by_user"
    user_id = columns.UUID(primary_key=True, required=True)
    event_time = columns.TimeUUID(primary_key=True, clustering_order="DESC", required=True)
    trip_id = columns.UUID(primary_key=True, clustering_order="DESC", required=True)
    foo = columns.Text()


def main():
    keyspace = "foo"

    print(TripEventByUser._columns.values())
    print("---")
    print(TripEventByUser()._columns.values())

    if os.getenv('CQLENG_ALLOW_SCHEMA_MANAGEMENT') is None:
        os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'

    connection.setup(['127.0.0.1'], keyspace, retry_connect=True, port=32774)

    # this is non-destructive
    # https://datastax.github.io/python-driver/api/cassandra/cqlengine/management.html#cassandra.cqlengine.management.create_keyspace_simple
    create_keyspace_simple(keyspace, 1)



    # this will create the table but if the model changes it doesn't seem to update an existing instance of same
    sync_table(TripEventByUser)


    trip_event_by_user_obj = TripEventByUser()

    # BaseTripEvent
    trip_event_by_user_obj.event_id = uuid.uuid4()
    trip_event_by_user_obj.event_type = "foo"
    trip_event_by_user_obj.lat = 1.1
    trip_event_by_user_obj.lng = 2.2
    trip_event_by_user_obj.accuracy = 3.3
    trip_event_by_user_obj.speed = 4.4
    trip_event_by_user_obj.meta = {"meta": 5.5}
    trip_event_by_user_obj.sdk = {"foo": "bar"}

    # TripEventByUser
    trip_event_by_user_obj.user_id = uuid.uuid4()
    trip_event_by_user_obj.event_time = uuid.uuid1()
    trip_event_by_user_obj.trip_id = uuid.uuid4()

    # new
    trip_event_by_user_obj.foo = "bar"

    # serialize
    trip_event_by_user_obj.save()


if __name__ == '__main__':
    main()
