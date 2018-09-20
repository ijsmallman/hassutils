import pytest
from os.path import join, dirname, exists
from datetime import datetime, timezone
import logging
from hassutils.database import DataBase


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def db_path():
    return join(
            dirname(__file__),
            'resources',
            'home-assistant_v2.db'
        )


def test_connect_to_database(db_path):

    with pytest.raises(ConnectionError):
        db = DataBase('madeup')

    with DataBase(db_path) as db:
        pass


def test_count_events(db_path):

    with DataBase(db_path) as db:
        events = db.count_events()
        assert events > 0


def test_count_states(db_path):
    with DataBase(db_path) as db:
        states = db.count_states()
        assert states > 0

        states = db.count_states('sensor.living_room_thermostat_temperature')
        assert states > 0


def test_fetch_temperature(db_path):
    with DataBase(db_path) as db:
        temps = db.fetch_temperature_readings()
        a=2

    # db_no_ext = tmpdir.join('{}'.format(uuid.uuid4()))
    # db_with_ext = tmpdir.join('{}.db'.format(uuid.uuid4()))
    # DataBase(db_no_ext.strpath)
    # DataBase(db_with_ext.strpath)
    # assert exists('{}.db'.format(db_no_ext))
    # assert exists('{}'.format(db_with_ext))


# def test_insert_vals_and_count_entries(tmpdir):
#     db_path = tmpdir.join('{}'.format(uuid.uuid4()))
#     db = DataBase(db_path.strpath)
#     n = 10
#     for i in range(n):
#         db.insert_val('test', datetime.utcnow(), i)
#     assert db.entry_count('test') == n
#     assert db.entry_count('made_up_name') == 0



