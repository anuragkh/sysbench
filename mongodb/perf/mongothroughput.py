#!/usr/bin/python

import sys
import getopt
import random
import threading
from datetime import datetime
from pymongo import MongoClient

writeLock = threading.Lock()


def secs(td):
  return (float(td.days) * 24.0 * 60.0 * 60.0 + float(td.seconds)) + float(td.microseconds) / (1000.0 * 1000.0)


class BenchmarkThread(threading.Thread):
  def __init__(self, thread_id, bench_type, mongo_server, db, collection, queries):
    threading.Thread.__init__(self)
    self.thread_id = thread_id
    self.bench_type = bench_type
    self.index = db
    self.doc_type = collection
    self.queries = queries
    print '[Thread %d] Connecting to MongoDB...' % thread_id
    self.client = MongoClient('mongodb://%s:27017' % mongo_server)
    self.col = self.client[db][collection]
    print '[Thread %d] Connected.' % thread_id
    self.query_count = len(queries)
    self.WARMUP_TIME = 10
    self.MEASURE_TIME = 30
    self.COOLDOWN_TIME = 10

  def bench_get(self):
    print '[Thread %d] Benchmarking get...' % self.thread_id

    qid = 0

    # Warmup
    print '[Thread %d] Warmup phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.WARMUP_TIME:
      query = self.queries[qid]
      self.col.find_one({'_id': query})
      qid = (qid + 1) % self.query_count

    # Measure
    print '[Thread %d] Measure phase...' % self.thread_id
    query_count = 0
    start = datetime.now()
    while secs(datetime.now() - start) < self.MEASURE_TIME:
      query = self.queries[qid]
      self.col.find_one({'_id': query})
      qid = (qid + 1) % self.query_count
      query_count += 1

    total_time = secs(datetime.now() - start)
    throughput = float(query_count) / total_time

    # Cooldown
    print '[Thread %d] Cooldown phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.COOLDOWN_TIME:
      query = self.queries[qid]
      self.col.find_one({'_id': query})
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
      res = self.col.find(query, {})
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
      res = self.col.find(query, {})
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
      res = self.col.find(query, {})
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
    else:
      print '[Thread %d] Error: Invalid bench_type %s.' % (self.thread_id, self.bench_type)
      sys.exit(2)

    writeLock.acquire()
    with open('throughput_%s' % self.bench_type, 'a') as out:
      out.write('%d\t%.2f\n' % (self.thread_id, throughput))
    writeLock.release()


def load_queries(bench_type, query_file, record_count):
  queries = []
  print '[Main Thread] Loading queries...'
  if bench_type == 'search':
    if query_file == '':
      print 'Error: Must specify query-file for search benchmark!'
      sys.exit(2)
    with open(query_file) as ifp:
      for line in ifp:
        field_id, query = line.strip().split('|', 2)
        qbody = {'field%s' % field_id: query}
        queries.append(qbody)
  elif bench_type == 'get':
    queries = random.sample(range(0, record_count), min(100000, record_count))
  else:
    print 'Error: Invalid benchtype %s' % bench_type

  return queries


def main(argv):
  mongo_server = 'localhost'
  query_file = ''
  database = 'bench'
  collection = 'data'
  bench_type = 'search'
  num_threads = 1
  help_msg = 'esbench.py -m <mongo-server> -q <queries> -d <database> -c <collection> -b <bench-type> -n <num-threads>'
  try:
    opts, args = getopt.getopt(argv, 'hm:q:d:c:b:',
                               ['mongo-server', 'queries=', 'database=', 'collection=', 'benchtype=', 'numthreads='])
  except getopt.GetoptError:
    print help_msg
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print help_msg
      sys.exit()
    elif opt in ('-m', '--mongo-server'):
      mongo_server = arg
    elif opt in ('-q', '--queries'):
      query_file = arg
    elif opt in ('-d', '--database'):
      database = arg
    elif opt in ('-c', '--collection'):
      collection = arg
    elif opt in ('-b', '--benchtype'):
      bench_type = arg
    elif opt in ('-n', '--numthreads'):
      num_threads = int(arg)

  client = MongoClient('mongodb://%s:27017' % mongo_server)
  count = client[database][collection].count()
  client.close()

  threads = []
  print '[Main Thread] Initializing %d threads...' % num_threads
  for i in range(0, num_threads):
    queries = load_queries(bench_type=bench_type, query_file=query_file, record_count=count)
    thread = BenchmarkThread(thread_id=i, bench_type=bench_type, mongo_server=mongo_server, db=database,
                             collection=collection, queries=queries)
    threads.append(thread)

  print '[Main Thread] Starting threads...'
  for thread in threads:
    thread.start()

  print '[Main Thread] Waiting for threads to join...'
  for thread in threads:
    thread.join()


if __name__ == '__main__':
  main(sys.argv[1:])
