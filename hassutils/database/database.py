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


# class ConnectionError(Exception):
#     pass

UNIT_MAP = {
    "celsius": "\u00b0C",
    "farenheit": "\u00B0F"
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
        self.conn = sqlite3.connect(
            self._db_path,
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        return self

    def __exit__(self, *_) -> None:
        if self.conn is not None:
            self.conn.close()

    def count_table_entries(self, table: str) -> int:
        query = "SELECT COUNT(*) from " + table
        c = self.conn.cursor()
        return c.execute(query).fetchone()[0]

    def count_events(self) -> int:
        return self.count_table_entries(EVENTS)

    def count_states(self, entity_id: str=None) -> int:
        if entity_id is None:
            return self.count_table_entries(STATES)
        else:
            query = "SELECT COUNT(*) from " + \
                     STATES + " " + \
                     "WHERE entity_id=?"
            c = self.conn.cursor()
            return c.execute(query, (entity_id,)).fetchone()[0]

    def fetch_temperature_readings(self,
                                   from_date: datetime.datetime=None,
                                   to_date: datetime.datetime=None,
                                   units: str='celcius') -> List[Tuple[str, str, float]]:

        query = "SELECT * from " + \
                STATES + " " + \
                "WHERE domain=? AND entity_id LIKE ?"
        c = self.conn.cursor()
        temps = c.execute(query, ["sensor", "%temperature%"]).fetchall()

        # [(name, time, temp),...]
        return [self.process_temp_entry(row) for row in temps]

    @staticmethod
    def process_temp_entry(entry, units: str= 'celsius') -> Tuple[str, str, float]:
        if units in UNIT_MAP.keys():
            units = UNIT_MAP[units]
        else:
            raise RuntimeError("Cannot convert temperatures into %s", units)

        metadata = json.loads(entry[4])

        # (name, time, temp)
        return (metadata["friendly_name"],
                entry[6],
                convert_temp_units(float(entry[3]), metadata["unit_of_measurement"], units))


def convert_temp_units(value: float, current_unit: str, target_unit: str) -> float:
    if current_unit == target_unit:
        return value
    elif target_unit == "\\u00b0C" and current_unit == "\\u00b0F":
        return (value - 32) * 5 / 9
    elif target_unit == "\\u00b0F" and current_unit == "\\u00b0C":
        return value * 9 / 5 + 32
    else:
        raise RuntimeError("Cant convert %s into %s", current_unit, target_unit)


    # def count_events(self, name=None) -> int:
    #     """
    #     Count entries in events table with given name.
    #
    #     Parameters
    #     ----------
    #     name: str
    #         Entry name to count. If none count all entries. Default=None
    #
    #     Returns
    #     -------
    #     counts: int
    #         Number of entries in events table.
    #     """
    #     if name is not None:
    #         query = "SELECT COUNT(*) from " + \
    #                 EVENTS + " " + \
    #                 "WHERE name=?"
    #         c = self.conn.cursor()
    #         rows = c.execute(query, (name,)).fetchone()[0]
    #     else:
    #         query = "SELECT COUNT(*) from " + EVENTS
    #         c = self.conn.cursor()
    #         rows = c.execute(query).fetchone()[0]
    #     return rows

    def entry_count(self, name: str) -> int:
        """
        Count database entries with given name
        
        Parameters
        ----------
        name: str
            entry name to count

        Returns
        -------
        counts: int
            number of entries with name in database
        """
        query = "SELECT COUNT(*) from " + \
                TABLE_NA + " " + \
                "WHERE name=?"
        c = self.conn.cursor()
        rows = c.execute(query, (name,)).fetchone()['COUNT(*)']
        return rows

    def fetch_entries(self,
                      from_datetime: datetime.datetime=None,
                      to_datetime: datetime.datetime=None,
                      filter_synced: bool=False) -> List[dict]:
        """
        Fetch entries from database
        
        Parameters
        ----------
        from_datetime: datetime.datetime
            start timestamp (default: None)
        to_datetime: datetime.datetime
            end timestamp (default: None)
        filter_synced: bool
            filter out all synced data (default: False)

        Returns
        -------
        entries: List[dict]
            database entries
        """
        c = self.conn.cursor()

        if (from_datetime is None) and (to_datetime is None):
            if filter_synced:
                c.execute("SELECT * FROM " +
                          TABLE_NAME + " " +
                          "WHERE [is-synced] = ?", (0,))
            else:
                c.execute("SELECT * FROM " + TABLE_NAME)

        elif from_datetime is None:
            if filter_synced:
                c.execute('SELECT * FROM ' +
                          TABLE_NAME + ' ' +
                          'WHERE timestamp <= ? ' +
                          'AND [is-synced] = ?', (to_datetime, 0))
            else:
                c.execute('SELECT * FROM ' +
                          TABLE_NAME + ' ' +
                          'WHERE [measured-at] <= ?', (to_datetime,))

        elif to_datetime is None:
            if filter_synced:
                c.execute('SELECT * FROM ' +
                          TABLE_NAME + ' ' +
                          'WHERE [measured-at] >= ? ' +
                          'AND [is-synced] = ?', (from_datetime, 0))
            else:
                c.execute('SELECT * FROM ' +
                          TABLE_NAME + ' ' +
                          'WHERE [measured-at] >= ?', (from_datetime,))
        else:
            if filter_synced:
                c.execute('SELECT * FROM ' +
                          TABLE_NAME + ' ' +
                          'WHERE [is-synced] = ? ' +
                          'AND [measured-at] BETWEEN ? AND ?', (0, from_datetime, to_datetime))
            else:
                c.execute('SELECT * FROM ' +
                          TABLE_NAME + ' ' +
                          'WHERE [measured-at] BETWEEN ? AND ?', (from_datetime, to_datetime))

        entries = c.fetchall()
        return entries

    def fetch_entry(self, timestamp: datetime) -> dict:
        """
        Fetch single entry from database by timestamp
        
        Parameters
        ----------
        timestamp: datetime.datetime
            timestamp for database entry

        Returns
        -------
        entry: dict
            the database entry
        """
        c = self.conn.cursor()
        c.execute('SELECT * FROM ' +
                  TABLE_NAME + ' ' +
                  'WHERE [measured-at] = ?',
                  (timestamp,))
        entry = c.fetchone()
        return entry

    def update_sync_status(self,
                           timestamp: datetime.datetime,
                           status: bool) -> None:
        c = self.conn.cursor()
        c.execute('UPDATE ' + TABLE_NAME + ' SET ' +
                  '[is-synced] = ? WHERE [measured-at] = ?',
                  (int(status), timestamp))
        self.conn.commit()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        if col[0] == 'is-synced':
            d[col[0]] = (True if row[idx] == 1 else False)
        else:
            d[col[0]] = row[idx]
    return d
