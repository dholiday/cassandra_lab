

import json

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.util import unix_time_from_uuid1



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