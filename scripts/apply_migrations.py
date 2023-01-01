import importlib
import pkgutil
import traceback

from mongomoron import query, insert_one, update_one

from db import conn, app_db_migration
from scripts import migrations

if __name__ == '__main__':
    # exit code
    c = 0

    cursor = conn.execute(query(app_db_migration))
    migration_history = dict((r['_id'], r) for r in cursor)

    for importer, mod_name, is_pkg in pkgutil.iter_modules(migrations.__path__):
        if mod_name in migration_history and migration_history[mod_name]['status'] == 'done':
            print("%s has been done already, skip" % mod_name)
            continue

        try:
            mod = importlib.import_module('%s.%s' % (migrations.__name__, mod_name))
            mod.upgrade(conn.db())
            print("%s has been done" % mod_name)
            migration = {'status': 'done'}
        except Exception as e:
            print("%s has failed" % mod_name)
            traceback.print_exc()
            migration = {'status': 'failed', 'error': str(e)}
            c = 1
        finally:
            conn.execute(update_one(app_db_migration,upsert=True)
                         .filter(app_db_migration._id == mod_name)
                         .set(migration))

    exit(c)
