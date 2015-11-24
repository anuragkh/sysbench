#!/usr/bin/python

import sys
import getopt
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch()
batch_size = 100


def csv2json(index, doc_type, id, csv):
  json = {'_index' : index, '_type': doc_type, '_id': id}
  fields = csv.split('|')
  for i in range(0, len(fields)):
    json['field%d' % i] = fields[i]
  return json


def load_data(input_file, index, doc_type, seed):
  doc_no = seed
  successful = 0
  docs = []
  with open(input_file) as ifp:
    for line in ifp:
      doc_id = str(doc_no)
      doc = csv2json(index, doc_type, doc_id, line.rstrip())
      docs.append(doc)
      doc_no += 1
      if len(docs) == batch_size:
        print docs
        docs_iter = iter(docs)
        (added, tmp) = helpers.bulk(es, docs_iter)
        successful += added
        docs = []
      if doc_no % 100000 == 0:
        print 'success: %d failed: %s' % (successful, doc_no - successful)

  if len(docs) > 0:
    print docs
    docs_iter = iter(docs)
    (added, tmp) = helpers.bulk(es, docs_iter)
    successful += added

  print 'Finished! Inserted: %d Failed: %d' % (successful, doc_no - successful)


def main(argv):
  input_file = ''
  index = 'bench'
  doc_type = 'data'
  seed = 0
  help_message = 'esload.py -d <data-file> -i <index> -t <type> -s <seed>'
  try:
    opts, args = getopt.getopt(argv, 'hd:i:t:s:', ['data=', 'index=', 'type=', 'seed='])
  except getopt.GetoptError:
    print help_message
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print help_message
      sys.exit()
    elif opt in ('-d', '--data'):
      input_file = arg
    elif opt in ('-i', '--index'):
      index = arg
    elif opt in ('-t', '--type'):
      doc_type = arg
    elif opt in ('-s', '--seed'):
      seed = int(arg)
  if input_file == '':
    print 'Error: Must specify data-file!'
    sys.exit(2)

  load_data(input_file, index, doc_type, seed)


if __name__ == '__main__':
  main(sys.argv[1:])
