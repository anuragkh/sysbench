#!/usr/bin/python

import getopt
import random
import sys
from datetime import datetime

from pymongo import MongoClient

client = None


def us(td):
  return (td.days * 24 * 60 * 60 + td.seconds) * 1000 * 1000 + td.microseconds


def bench_search(query_file, col):
  with open(query_file) as ifp:
    for line in ifp:
      field_id, query = line.strip().split('|', 2)
      count = 0
      start = datetime.now()
      qbody = {'field%s' % field_id: query}
      res = col.find(qbody, {})
      for _ in res:
        count += 1
      end = datetime.now()
      print '%d\t%d' % (count, us(end - start))


def bench_get(record_count, col):
  ids = random.sample(range(0, record_count), min(100000, record_count))
  for i in ids:
    start = datetime.now()
    res = col.find_one({'_id': i})
    length = len(res)
    end = datetime.now()
    print '%d\t%s\t%d' % (i, length, us(end - start))


def main(argv):
  mongo_server = 'localhost'
  query_file = ''
  database = 'bench'
  collection = 'data'
  bench_type = 'search'
  help_msg = 'mongobench.py -m <mongo-server> -q <queries> -d <database> -c <collection> -b <bench-type>'
  try:
    opts, args = getopt.getopt(argv, 'hm:q:d:c:b:', ['mongo-server', 'queries=', 'database=', 'collection=', 'benchtype='])
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
    elif opt in ('-i', '--database'):
      database = arg
    elif opt in ('-c', '--collection'):
      collection = arg
    elif opt in ('-b', '--benchtype'):
      bench_type = arg
  if bench_type == 'search' and query_file == '':
    print 'Error: Must specify query-file for search benchmark!'
    sys.exit(2)

  global client
  client = MongoClient('mongodb://%s:27017' % mongo_server)
  db = client[database]
  col = db[collection]
  record_count = col.count()

  if bench_type == 'search':
    bench_search(query_file, col)
  elif bench_type == 'get':
    bench_get(record_count, col)


if __name__ == '__main__':
  main(sys.argv[1:])
