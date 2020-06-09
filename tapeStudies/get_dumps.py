import os, sys,re,glob,bz2
import urllib.request,ssl
import shutil

import logging
import argparse
import datetime


# python tool to download rucio dumps file to a bz2 compressed text file

#date is in %d-%m-%Y format
url_replicas_per_rse="https://rucio-hadoop.cern.ch/replica_dumps?rse={rse}&date={date}"


parser = argparse.ArgumentParser()
parser.add_argument("--rses", nargs='+',default=['RAL-LCG2_DATATAPE','RAL-LCG2_MCTAPE'],
                    help="space-separated list of RSEs")
parser.add_argument("-d",'--directory', default=".",
                    help='Output file directory')

#parser.add_argument("-n",default=False,action='store_true',
#                    help="Don't download the file")

def download_from_url(url, fname):
    """Download file from url and write as fname
    """
    print(url, fname)
    gcontext = ssl.SSLContext()
    with urllib.request.urlopen(url,context=gcontext) as response, open(fname, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)


def bz2_to_dict(fname):
    #RSE, scope, name, checksum, size, creation date, path, update date, state, last accessed date, tombstone
    with bz2.open(fname,'rb') as f:
        for line in f.readlines():
            print(len(line.split('\t')))


if __name__ == "__main__":
    args = parser.parse_args()
    today = datetime.datetime.now()
    for rse in args.rses:
        fname = f'{args.directory}/{rse}_{today.strftime("%d-%m-%Y")}.bz2'
        download_from_url( url_replicas_per_rse.format(rse=rse,
                                    date = today.strftime("%d-%m-%Y") ),
                                    fname)
        #bz2_to_dict(fname)
