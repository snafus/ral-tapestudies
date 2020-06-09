import os, sys,re,glob,bz2
import urllib.request,ssl
import shutil

import logging
import argparse
import datetime

# from a rucio dumps file, and list of datasets names, create an output file with the 
# list of replicas, with the full castor path (by default) prepended

parser = argparse.ArgumentParser()
parser.add_argument('-r',"--dumpfile",
                    help='Input dump file (if bz2, then assume compressed, else not)')

parser.add_argument('-d',"--datasets",nargs='+',
                    help='List of dataset names')
parser.add_argument('-f',"--inputfile",type=str,default=None,
                    help='file of dataset names')                  

parser.add_argument('-o',"--output",type=str,
                    help='output file with list of replicas')

parser.add_argument('-p',"--prependpath",default=False,action='store_true',
                    help='Prepend the castor full path to output paths')


data_path = '/castor/ads.rl.ac.uk/prod/atlas/raw/atlasdatatape'


def input_open(fname):
    dumpfile_is_compressed = True if '.bz2' in args.dumpfile else False
    if dumpfile_is_compressed:
        with bz2.open(fname,'rb') as f:
            for line in f.readlines():
                yield line.decode('utf-8').split('\t')[6]
    else:
        with open(fname,'r') as f:
            for line in f.readlines():
                yield line.split('\t')[6]


def get_datasets():
    global args
    if args.datasets is not None and len(args.datasets):
        inputdatasets = [x.strip() for x in args.datasets]
    elif args.inputfile is not None:
        with open(args.inputfile,'r') as f:
            inputdatasets = [x.strip() for x in f.readlines()]
    inputdatasets = [x.split(":")[-1] for x in inputdatasets]
    print(inputdatasets)
    return inputdatasets

if __name__ == "__main__":
    args = parser.parse_args()
    today = datetime.datetime.now()

    #inputdatasets = [x.strip() for x in args.datasets]
    #inputdatasets = [x.split(":")[-1] for x in inputdatasets]
    inputdatasets = get_datasets()
    print(f'Found {len(inputdatasets)} input datasets to search for')


    print("Begin search")
    counter = 0
    match_counter = 0
    with open(args.output,'w') as foo:
        for path in input_open(args.dumpfile):
            if counter % 1000000 == 0:
                print(f'Counter: {counter}')
            if any( (ds in path) for ds in inputdatasets):
                if args.prependpath:
                    ppath = '/'.join([data_path,path])
                else:
                    ppath = path
                print(path)
                foo.write(ppath+'\n')
                match_counter += 1
            counter += 1
    print("End search")
    print(f'Found {match_counter} matches; writen to {args.output}')

