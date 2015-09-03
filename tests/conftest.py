from ognom.connection import ConnectionManager


def pytest_configure(config):
    connection_manager = ConnectionManager()
    connection_manager.connect({'main': {
        'name': 'master_common_test',
        'args': ['127.0.0.1:27017']}})

