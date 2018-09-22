from os.path import exists
import sqlite3
import datetime
import logging
import json
from typing import List, Tuple

logger = logging.getLogger(__name__)


EVENTS = "events"
RECORDER_RUNS = "recorder_runs"
SCHEMA_CHANGES = "schema_changes"
STATES = "states"


UNIT_MAP = {
    "celsius": "\u00b0C",
    "fahrenheit": "\u00B0F"
}


class DataBase:

    def __init__(self, db_path: str) -> None:
        """
        Initialise data base object.

        Parameters
        ----------
        db_path: str
            Path to sqlite database file.
        """

        if not exists(db_path):
            raise ConnectionError("Cannot connect to DB, file %s does not exist", db_path)

        self._db_path = db_path
        self.conn = None

        logger.debug("Connected to DB %s", db_path)

    def __enter__(self) -> 'DataBase':
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.disconnect()

    def connect(self):
        """
        Connect to database.
        """
        self.conn = sqlite3.connect(
            self._db_path,
            detect_types=sqlite3.PARSE_DECLTYPES
        )

    def disconnect(self):
        """
        Disconnect from database.
        """
        if self.conn is not None:
            self.conn.close()

    def count_table_entries(self, table: str) -> int:
        """
        Count entries in given table.

        Parameters
        ----------
        table: str
            Name of table.

        Returns
        -------
        count: int
            Number of entries.
        """
        query = "SELECT COUNT(*) from " + table
        c = self.conn.cursor()
        return c.execute(query).fetchone()[0]

    def count_events(self) -> int:
        """
        Count entries in 'Events' table.

        Returns
        -------
        count: int
            Number of entries.
        """
        return self.count_table_entries(EVENTS)

    def count_states(self, entity_id: str=None) -> int:
        """
        Number of entries in 'States' table.

        Parameters
        ----------
        entity_id: str
            Optionally filter table by entity ID (default: None).

        Returns
        -------
        count: int
            Number of entries.
        """
        if entity_id is None:
            return self.count_table_entries(STATES)
        else:
            query = "SELECT COUNT(*) from " + \
                     STATES + " " + \
                     "WHERE entity_id=?"
            c = self.conn.cursor()
            return c.execute(query, (entity_id,)).fetchone()[0]

    def fetch_temperature_readings(self,
                                   from_date: str=None,
                                   to_date: str=None,
                                   units: str='celsius') -> List[Tuple[str, str, float]]:
        """
        Fetch temperature readings from all appropriate sensors.

        Parameters
        ----------
        from_date: str
            Datetime string (default: None).
        to_date: str
            Datetime string (default: None).
        units: str
            Units to output temperature in. Supported units: [celsius, fahrenheit]

        Returns
        -------
        [(friendly_name, time_stamp, temperature),...]: List[Tuple[str, datetime.datetime, float]]
            List of temperature readings.
        """

        parameters = [p for p in ["sensor", "%temperature%", from_date, to_date]
                      if p is not None]

        from_filter = "AND strftime(\"%s\", last_changed) >= strftime(\"%s\", ?) " \
            if from_date is not None else ""
        to_filter = "AND strftime(\"%s\", last_changed) <= strftime(\"%s\", ?)" \
            if to_date is not None else ""

        query = "SELECT * from " + \
                STATES + " " + \
                "WHERE domain=? AND entity_id LIKE ? " + \
                from_filter + \
                to_filter
        c = self.conn.cursor()
        temps = c.execute(query, parameters).fetchall()

        # [(name, time, temp),...]
        return [self.process_temp_entry(row, units) for row in temps]

    @staticmethod
    def process_temp_entry(entry: Tuple[int, str, str, str, str, int, str, str, str, str, None],
                           units: str= 'celsius') -> Tuple[str, str, float]:
        """
        Process temperature entry in the 'States' table.

        Parameters
        ----------
        entry: Tuple[int, str, str, str, str, int, str, str, str, str, None]
            Entry in 'States' table.
        units: str
            Units to output temperature in. Supported units: [celsius, fahrenheit]

        Returns
        -------
        (friendly_name, time_stamp, temperature): Tuple[str, datetime.datetime, float]
            Processed table entry.
        """
        if units.lower() in UNIT_MAP.keys():
            units = UNIT_MAP[units.lower()]
        else:
            logger.warn("Cannot convert temperatures into %s. Defaulting to celsius.", units)

        metadata = json.loads(entry[4])

        # (name, time, temp)
        entry = (metadata["friendly_name"],
                 datetime.datetime.strptime(entry[6], '%Y-%m-%d %H:%M:%S.%f'),
                 convert_temp_units(float(entry[3]), metadata["unit_of_measurement"], units))

        return entry


def convert_temp_units(value: float, current_unit: str, target_unit: str) -> float:
    """
    Convert units of temperature reading.

    Parameters
    ----------
    value
    current_unit
    target_unit

    Returns
    -------

    """
    if current_unit == target_unit:
        return value
    elif target_unit == "\\u00b0C" and current_unit == "\\u00b0F":
        return (value - 32) * 5 / 9
    elif target_unit == "\\u00b0F" and current_unit == "\\u00b0C":
        return value * 9 / 5 + 32
    else:
        raise RuntimeError("Cant convert %s into %s", current_unit, target_unit)
