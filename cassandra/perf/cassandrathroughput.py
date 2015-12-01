#!/usr/bin/python

import sys
import getopt
import random
import threading
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

cluster = None
writeLock = threading.Lock()


def secs(td):
  return (float(td.days) * 24.0 * 60.0 * 60.0 + float(td.seconds)) + float(td.microseconds) / (1000.0 * 1000.0)


class BenchmarkThread(threading.Thread):
  def __init__(self, thread_id, bench_type, keyspace, queries):
    threading.Thread.__init__(self)
    self.thread_id = thread_id
    self.bench_type = bench_type
    self.queries = queries
    print '[Thread %d] Connecting to Cassandra...' % thread_id
    self.session = cluster.connect(keyspace)
    print '[Thread %d] Connected.' % thread_id
    self.query_count = len(queries)
    self.WARMUP_TIME = 60
    self.MEASURE_TIME = 120
    self.COOLDOWN_TIME = 60

  def bench_get(self):
    print '[Thread %d] Benchmarking get...' % self.thread_id

    qid = 0

    # Warmup
    print '[Thread %d] Warmup phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.WARMUP_TIME:
      query = self.queries[qid]
      self.session.execute(query)
      qid = (qid + 1) % self.query_count

    # Measure
    print '[Thread %d] Measure phase...' % self.thread_id
    query_count = 0
    start = datetime.now()
    while secs(datetime.now() - start) < self.MEASURE_TIME:
      query = self.queries[qid]
      self.session.execute(query)
      qid = (qid + 1) % self.query_count
      query_count += 1

    total_time = secs(datetime.now() - start)
    throughput = float(query_count) / total_time

    # Cooldown
    print '[Thread %d] Cooldown phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.COOLDOWN_TIME:
      query = self.queries[qid]
      self.session.execute(query)
      qid = (qid + 1) % self.query_count

    print '[Thread %d] Benchmark complete.' % self.thread_id
    return throughput

  def bench_search(self):
    print '[Thread %d] Benchmarking search...' % self.thread_id

    qid = 0

    # Warmup
    print '[Thread %d] Warmup phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.WARMUP_TIME:
      query = self.queries[qid]
      count = 0
      res = self.session.execute(query)
      for _ in res:
        count += 1
      qid = (qid + 1) % self.query_count

    # Measure
    print '[Thread %d] Measure phase...' % self.thread_id
    query_count = 0
    start = datetime.now()
    while secs(datetime.now() - start) < self.MEASURE_TIME:
      query = self.queries[qid]
      count = 0
      res = self.session.execute(query)
      for _ in res:
        count += 1
      qid = (qid + 1) % self.query_count
      query_count += 1

    total_time = secs(datetime.now() - start)
    throughput = float(query_count) / total_time

    # Cooldown
    print '[Thread %d] Cooldown phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.COOLDOWN_TIME:
      query = self.queries[qid]
      count = 0
      res = self.session.execute(query)
      for _ in res:
        count += 1
      qid = (qid + 1) % self.query_count

    print '[Thread %d] Benchmark complete.' % self.thread_id

    return throughput

  def bench_get_search(self):
    print '[Thread %d] Benchmarking search...' % self.thread_id

    qid = 0

    # Warmup
    print '[Thread %d] Warmup phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.WARMUP_TIME:
      query = self.queries[qid]
      if qid % 2 == 0:
        self.session.execute(query)
      else:
        count = 0
        res = self.session.execute(query)
        for _ in res:
          count += 1
      qid = (qid + 1) % self.query_count

    # Measure
    print '[Thread %d] Measure phase...' % self.thread_id
    query_count = 0
    start = datetime.now()
    while secs(datetime.now() - start) < self.MEASURE_TIME:
      query = self.queries[qid]
      if qid % 2 == 0:
        self.session.execute(query)
      else:
        count = 0
        res = self.session.execute(query)
        for _ in res:
          count += 1
      qid = (qid + 1) % self.query_count
      query_count += 1

    total_time = secs(datetime.now() - start)
    throughput = float(query_count) / total_time

    # Cooldown
    print '[Thread %d] Cooldown phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.COOLDOWN_TIME:
      query = self.queries[qid]
      if qid % 2 == 0:
        self.session.execute(query)
      else:
        count = 0
        res = self.session.execute(query)
        for _ in res:
          count += 1
      qid = (qid + 1) % self.query_count

    print '[Thread %d] Benchmark complete.' % self.thread_id

    return throughput

  def run(self):
    if self.bench_type == 'get':
      throughput = self.bench_get()
    elif self.bench_type == 'search':
      throughput = self.bench_search()
    elif self.bench_type == 'get-search':
      throughput = self.bench_get_search()
    else:
      print '[Thread %d] Error: Invalid bench_type %s.' % (self.thread_id, self.bench_type)
      sys.exit(2)

    writeLock.acquire()
    with open('throughput_%s' % self.bench_type, 'a') as out:
      out.write('%d\t%.2f\n' % (self.thread_id, throughput))
    writeLock.release()


def load_queries(bench_type, query_file, table, record_count):
  queries = []
  print '[Main Thread] Loading queries...'
  if bench_type == 'search':
    if query_file == '':
      print 'Error: Must specify query-file for search benchmark!'
      sys.exit(2)
    with open(query_file) as ifp:
      for line in ifp:
        field_id, field_value = line.strip().split('|', 2)
        field_key = 'field%s' % field_id
        query = 'SELECT id FROM %s WHERE (%s = \'%s\')' % (table, field_key, field_value)
        queries.append(query)
    queries = random.sample(queries, min(100000, len(queries)))
  elif bench_type == 'get':
    for x in random.sample(range(0, record_count), min(100000, record_count)):
      queries.append('SELECT * FROM %s WHERE (id = %s)' % (table, x))
  elif bench_type == 'get-search':
    # Get get() queries
    get_queries = []
    for x in random.sample(range(0, record_count), min(100000, record_count)):
      get_queries.append('SELECT * FROM %s WHERE (id = %s)' % (table, x))

    # Get search() queries
    search_queries = []
    if query_file == '':
      print 'Error: Must specify query-file for search benchmark!'
      sys.exit(2)
    with open(query_file) as ifp:
      for line in ifp:
        field_id, field_value = line.strip().split('|', 2)
        field_key = 'field%s' % field_id
        query = 'SELECT id FROM %s WHERE (%s = \'%s\')' % (table, field_key, field_value)
        search_queries.append(query)
    search_queries = random.sample(search_queries, min(100000, len(search_queries)))

    query_count = max(len(get_queries), len(search_queries)) * 2
    for i in range(0, query_count):
      if i % 2 == 0:
        queries.append(get_queries[i / 2])
      else:
        queries.append(search_queries[i / 2])

  else:
    print 'Error: Invalid benchtype %s' % bench_type

  return queries


def main(argv):
  cassandra_server = 'localhost'
  query_file = ''
  keyspace = 'bench'
  table = 'data'
  bench_type = 'search'
  num_threads = 1
  record_count = -1
  help_msg = 'esbench.py -c <cassandra-server> -q <queries> -k <keyspace> -t <table> -b <bench-type> -n <num-threads> -r <record-count>'
  try:
    opts, args = getopt.getopt(argv, 'hc:q:k:t:b:r:',
                               ['cassandra-server', 'queries=', 'keyspace=', 'table=', 'benchtype=', 'numthreads=', 'recordcount='])
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
    elif opt in ('-n', '--numthreads'):
      num_threads = int(arg)
    elif opt in ('-r', '--recordcount'):
      record_count = int(arg)

  global cluster
  cluster = Cluster([cassandra_server])
  session = cluster.connect(keyspace)
  session.row_factory = dict_factory
  if record_count == -1:
    count = session.execute('SELECT count(id) FROM %s' % table)[0]['system.count(id)']
  else:
    count = record_count

  threads = []
  print '[Main Thread] Initializing %d threads...' % num_threads
  for i in range(0, num_threads):
    queries = load_queries(bench_type=bench_type, query_file=query_file, table=table, record_count=count)
    thread = BenchmarkThread(thread_id=i, bench_type=bench_type, keyspace=keyspace,
                             queries=queries)
    threads.append(thread)

  print '[Main Thread] Starting threads...'
  for thread in threads:
    thread.start()

  print '[Main Thread] Waiting for threads to join...'
  for thread in threads:
    thread.join()

  cluster.shutdown()


if __name__ == '__main__':
  main(sys.argv[1:])
