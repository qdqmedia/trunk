# -*- coding: utf-8  -*-
try:
    from Queue import Empty, Full
except ImportError:
    from queue import Empty, Full  # noqa

from trunk import Trunk


class PGQueue(object):
    def __init__(self, dsn):
        self.trunk = Trunk(dsn)

    def create(self, name):
        self.trunk.listen(name)
        # This forces db trigger to resend notifications about pending messages.
        with self.trunk.cursor() as cursor:
            cursor.execute("UPDATE trunk_queue SET locked_at = NULL WHERE name = %s AND locked_at IS NULL", (name, ))

    def get(self, name, block=True, timeout=None):
        channel, payload = self.trunk.get(name, block=block, timeout=timeout)
        with self.trunk.cursor() as cursor:
            cursor.execute('UPDATE trunk_queue SET locked_at = (CURRENT_TIMESTAMP) '
                           'WHERE id = %s AND name = %s AND locked_at is NULL', (payload, name))
            row_count = cursor.rowcount
            if not row_count:
                raise Empty()
            cursor.execute("SELECT id, message FROM trunk_queue WHERE id = %s", (payload, ))
            row = cursor.fetchone()
            if row is None:
                raise Empty()
            return row

    def get_nowait(self, name):
        return self.get(name, block=False)

    def put(self, name, message):
        with self.trunk.cursor() as cursor:
            cursor.execute("INSERT INTO trunk_queue (name, message) VALUES (%s, %s)", (name, message))

    def empty(self, name):
        return 0 == self.qsize(name)

    def qsize(self, name):
        with self.trunk.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM trunk_queue WHERE name = %s", (name,))
            row = cursor.fetchone()
            return row[0]

    def purge(self, name):
        size = self.qsize(name)
        with self.trunk.cursor() as cursor:
            cursor.execute("DELETE FROM trunk_queue WHERE name = %s", (name,))
        return size

    def close(self):
        self.trunk.close()
