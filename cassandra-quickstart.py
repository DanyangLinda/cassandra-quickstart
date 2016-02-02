import datetime

from cassandra.cluster import Cluster, SimpleStatement
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra.util import *

import json

BATCH_SIZE=10

def format_value(v):
    if type (v) is str:
        return v
    elif type(v) is datetime:
        return datetime.strftime(v, "%Y-%m-%d %H:%M:%S")
    else:
        return str(v)


def setup_keyspace (dc,rf):

    # If keyspace or already exists C* will just ignore the statements.

    session.execute(("CREATE KEYSPACE IF NOT EXISTS sensor_data "
                       "WITH replication = {{'class': 'NetworkTopologyStrategy', '{dc}': '{rf}'}} "
                       "AND durable_writes = true; ").format(dc=dc,rf=rf))

    cf = SimpleStatement("""
        CREATE TABLE IF NOT EXISTS sensor_data.raw (
            device text,
            sensor text,
            time timestamp,
            metric double,
            PRIMARY KEY ((device, sensor), time)
        ) WITH CLUSTERING ORDER BY (time DESC)
            AND bloom_filter_fp_chance = 0.01
            AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
            AND comment = ''
            AND compaction = {'max_sstable_age_days': '0.05', 'base_time_seconds': '300', 'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'}
            AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND dclocal_read_repair_chance = 0.1
            AND default_time_to_live = 0
            AND gc_grace_seconds = 0
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99.0PERCENTILE';
        """)

    session.execute(cf)

    # Example of how to show keyspace and table metadata with python cql:
    keyspace = cluster.metadata.keyspaces['sensor_data']
    table = keyspace.tables['raw']

    print keyspace.as_cql_query()
    print table.as_cql_query(True)



def insert_data():
    # Example showing how to read data from a csv and insert into Cassandra using a prepared statement

    insert_statement = session.prepare("INSERT INTO sensor_data.raw (device, sensor, time, metric) VALUES (?,?,?,?)")

    f = open("rows_input.csv")
    rows = f.read().splitlines()
    f.close()

    rows_inserted = 0

    # Example showing how to use Cassandra batched insert
    batch_insert = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for row in rows:
        data = row.split(',')
        values=[data[0],data[1],datetime.datetime.strptime(data[2], "%Y-%m-%d %H:%M:%S"),float(data[3])]
        insert = insert_statement.bind(values)
        batch_insert.add(insert)

        rows_inserted += 1

        if rows_inserted % BATCH_SIZE == 0:
            session.execute_async(batch_insert)

    print "Inserted %d rows" %rows_inserted

    return rows_inserted

def select_data():

    statement = session.prepare("SELECT device, sensor, time, metric "
                                "FROM sensor_data.raw "
                                "WHERE device=? and sensor=? and time=?")
    statement.consistency_level=ConsistencyLevel.ONE

    # Example showing how to select data and print to stdout
    try:
        device='328faaaa-f9a8-42b9-9fcd-4fb6ad70e843'
        times=["2016-02-01 11:59:36",
               "2016-02-01 11:58:56",
               "2016-02-01 11:58:16",
               "2016-02-01 11:57:36"]
        sensor='temperature'

        for time in times:
            select = statement.bind([device,sensor,datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S")])

            results=session.execute(select)

            if len(results) == 0:
                return 0
            else:
                for row in results:
                    print "%s | %s | %s " % (row.device, row.time, row.metric)

    except Exception as e:
        print "Error: %s" %( e)

    return 0

########### main ###############

c = open("configuration.json")
configuration = json.loads(c.read())
c.close()

cluster_config = configuration["cluster"]

cluster = Cluster(
    contact_points=cluster_config["contact_points"],
    auth_provider=PlainTextAuthProvider(
        username=cluster_config["username"],
        password=cluster_config["password"])
)
session = cluster.connect()
session.default_timeout = 120

print 'Connected to cluster %s' % cluster.metadata.cluster_name
for host in cluster.metadata.all_hosts():
    print 'Datacenter: %s; Host: %s; Rack: %s' % (host.datacenter, host.address, host.rack)

setup_keyspace(cluster_config["data_centres"][0], 3)

insert_data()
select_data()

cluster.shutdown()
