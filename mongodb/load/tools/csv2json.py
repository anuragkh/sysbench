#!/usr/bin/python
import csv
from optparse import OptionParser


# converts a array of csv-columns to a mongodb json line
def convert_csv_to_json(id, csv_line):
  json_elements = ['_id : %d' % id]

  for index in range(0, len(csv_line)):
    json_elements.append('field%d' % index + ': \"' + unicode(csv_line[index], 'UTF-8') + '\"')

  line = "{ " + ', '.join(json_elements) + " }"
  return line


# parsing the commandline options
parser = OptionParser(description="Parses a csv-file and converts it to mongodb json format.")
parser.add_option("-c", "--csvfile", dest="csvfile", action="store", help="input csvfile")
parser.add_option("-j", "--jsonfile", dest="jsonfile", action="store", help="json output file")
parser.add_option("-d", "--delimiter", dest="delimiter", action="store", help="csvdelimiter")
parser.add_option("-s", "--seed", dest="seed", action="store", help="seed", default=0)

(options, args) = parser.parse_args()

# parsing and converting the csvfile
csvreader = csv.reader(open(options.csvfile, 'rb'), delimiter=options.delimiter)
jsonfile = open(options.jsonfile, 'wb')
doc_no = options.seed
while True:
  try:
    csv_current_line = csvreader.next()
    json_current_line = convert_csv_to_json(doc_no, csv_current_line)
    print >> jsonfile, json_current_line
    doc_no += 1

  except csv.Error as e:
    print "Error parsing csv: %s" % e
  except StopIteration as e:
    print "=== Finished ==="
    break

jsonfile.close()
