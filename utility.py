from __future__ import print_function
import sys
import psycopg2
import psycopg2.extras

host = input("host=")
dbname = input("dbname=")
user = input("user=")
password=input("password=")


conn = psycopg2.connect(f'host={host} dbname={dbname} user={user} password={password}',
   connection_factory=psycopg2.extras.LogicalReplicationConnection)
cur = conn.cursor()
try:
    # test_decoding produces textual output
    cur.start_replication(slot_name='pytest', decode=True)
except psycopg2.ProgrammingError:
    cur.create_replication_slot('pytest', output_plugin='test_decoding')
    cur.start_replication(slot_name='pytest', decode=True)

class DemoConsumer(object):
    def __call__(self, msg):
        print(msg.payload)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

democonsumer = DemoConsumer()

print("Starting streaming, press Control-C to end...", file=sys.stderr)
try:
   cur.consume_stream(democonsumer)
except KeyboardInterrupt:
   cur.close()
   conn.close()
   print("The slot 'pytest' still exists. Drop it with "
      "SELECT pg_drop_replication_slot('pytest'); if no longer needed.",
      file=sys.stderr)
   print("WARNING: Transaction logs will accumulate in pg_xlog "
      "until the slot is dropped.", file=sys.stderr)
