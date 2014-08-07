import os
import re
import sys
 
from optparse import OptionParser

KEYSPACE="Keyspace"
COLUMNFAMILY = "Column Family";
READCOUNT = "Read Count";
READLATENCY = "Read Latency";
WRITECOUNT = "Write Count";
WRITELATENCY = "Write Latency";
PENDINGTASKS = "Pending Tasks";
SSTABLECOUNT = "SSTable count";
SSTABLEIEL = "SSTables in each level";
SPACEUSEDL = "Space used (live)";
SPACEUSEDT = "Space used (total)";
SSTABLECR = "SSTable Compression Ratio";
NUMKEYS = "Number of Keys (estimate)";
MEMTABLECC = "Memtable Columns Count";
MEMTABLEDS = "Memtable Data Size";
MEMTABLESC = "Memtable Switch Count";
BLOOMFFP = "Bloom Filter False Positives";
BLOOMFFR = "Bloom Filter False Ratio";
BLOOMFSU = "Bloom Filter Space Used";
COMPACTEDRMINS = "Compacted row minimum size";
COMPACTEDRMAXS = "Compacted row maximum size";
COMPACTEDRMEANS = "Compacted row mean size";


COLUMN_COUNT = 20

INDEX_MAP = {
"SSTable count":1,
"Space used (live)": 2,
"Space used (total)": 3,
"Number of Keys (estimate)": 4,
"Memtable Columns Count": 5,
"Memtable Data Size": 6,
"Memtable Switch Count": 7,
"Read Count": 8,
"Read Latency": 9,
"Write Count": 10,
"Write Latency": 11,
"Pending Tasks": 12,
"Bloom Filter False Postives": 13,
"Bloom Filter False Ratio": 14,
"Bloom Filter Space Used": 15,
"Compacted row minimum size": 16,
"Compacted row maximum size": 17,
"Compacted row mean size": 18
}


def createHeaderRow():
    HeaderList  = ["Row Type","Entity","SSTable count","Space used (live)","Space used (total)","Number of Keys (estimate)","Memtable Columns Count","Memtable Data Size","Memtable Switch Count","Read Count","Read Latency","Write Count","Write Latency","Pending Tasks","Bloom Filter False Postives","Bloom Filter False Ratio","Bloom Filter Space Used","Compacted row minimum size","Compacted row maximum size","Compacted row mean size"]
    return HeaderList


def parseAndFormatData(data,fp):
    lines = data.split('\n')
    
    csvList = [];i=0
    while i < COLUMN_COUNT:
        csvList.append('')
        i += 1
    fp.write('"'+'","'.join(createHeaderRow())+'"'+"\n")
    for each in lines:
        if COLUMNFAMILY in each or KEYSPACE in each:
            #new row in csv
            if csvList[0] != '':
		fp.write('"'+'","'.join(csvList)+'"'+"\n")
                while i < COLUMN_COUNT:
                    csvList.append('')
                    i += 1

            csvList[0],csvList[1]=each.split(':')
       	    csvList[0] =  csvList[0].lstrip()
        else:
            if ":" in each:
		each = each.lstrip()
                elemntsList = each.split(":")
                elemntsList[0].lstrip()
                if elemntsList[0] in INDEX_MAP.keys():
			if "NaN " in elemntsList[1]:
				elemntsList[1]=""
		csvList[INDEX_MAP[elemntsList[0]]+1]=elemntsList[1].strip()
                else:
                    continue
            else:
                continue

    sys.exit(1) 
 
def main():
    usage = 'usage: %prog --input=<path to a file with cfhistograms output>' + \
             ' --output=<path to the output directory>'
    parser = OptionParser(usage=usage)
    parser.add_option('--input', dest='input',
                  help='Path to a file with cfhistograms output')
    parser.add_option('--output', dest='output',
                  help='Path to a file where the graphs are saved')
 
    (options, args) = parser.parse_args()
 
    if not options.input:
        print('Error: Missing "--input" option')
        print parser.print_usage()
        sys.exit(1)
 
    if not options.output:
        print('Error: Missing "--output" option')
        print parser.print_usage()
        sys.exit(1)
 
    if not os.path.exists(options.input) or not \
	os.path.isfile(options.input):
        print('--input argument is not a valid file path')
        sys.exit(2)
 
    fp1 = open(options.output, 'wb')
    with open(options.input, 'r') as fp:
        print('Processing file...') 
        content = fp.read()
        parseAndFormatData(content,fp1)
    fp1.close()
main()

