#!/usr/bin/python

import getopt
import random
import sys
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

session = None


def us(td):
  return (td.days * 24 * 60 * 60 + td.seconds) * 1000 * 1000 + td.microseconds


def bench_search(query_file, table):
  with open(query_file) as ifp:
    for line in ifp:
      field_id, field_value = line.strip().split('|', 2)
      count = 0
      start = datetime.now()
      field_key = 'field%s' % field_id
      query = 'SELECT id FROM %s WHERE (%s = \'%s\')' % (table, field_key, field_value)
      res = session.execute(query)
      for _ in res:
        count += 1
      end = datetime.now()
      print '%d\t%d' % (count, us(end - start))


def bench_get(record_count, table):
  ids = random.sample(range(0, record_count), min(100000, record_count))
  for i in ids:
    start = datetime.now()
    res = session.execute('SELECT * FROM %s WHERE (id = %s)' % (table, i))[0]
    length = len(res)
    end = datetime.now()
    print '%d\t%s\t%d' % (i, length, us(end - start))


def main(argv):
  cassandra_server = 'localhost'
  query_file = ''
  keyspace = 'bench'
  table = 'data'
  bench_type = 'search'
  count = -1
  help_msg = 'cassandrabench.py -c <cassandra-server> -q <queries> -k <keyspace> -t <table> -b <bench-type> -r <record-count>'
  try:
    opts, args = getopt.getopt(argv, 'hc:q:k:t:b:r:',
                               ['cassandra-server', 'queries=', 'keyspace=', 'table=', 'benchtype=', 'recordcount='])
  except getopt.GetoptError:
    print help_msg
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print help_msg
      sys.exit()
    elif opt in ('-c', '--cassandra-server'):
      cassandra_server = arg
    elif opt in ('-q', '--queries'):
      query_file = arg
    elif opt in ('-k', '--keyspace'):
      keyspace = arg
    elif opt in ('-t', '--table'):
      table = arg
    elif opt in ('-b', '--benchtype'):
      bench_type = arg
    elif opt in ('-r', '--recordcount'):
      count = int(arg)
  if bench_type == 'search' and query_file == '':
    print 'Error: Must specify query-file for search benchmark!'
    sys.exit(2)

  cluster = Cluster([cassandra_server])
  global session
  session = cluster.connect(keyspace)
  session.row_factory = dict_factory
  if count == -1:
    record_count = session.execute('SELECT count(id) FROM %s' % table)[0]['system.count(id)']
  else:
    record_count = count

  if bench_type == 'search':
    bench_search(query_file, table)
  elif bench_type == 'get':
    bench_get(record_count, table)


if __name__ == '__main__':
  main(sys.argv[1:])
