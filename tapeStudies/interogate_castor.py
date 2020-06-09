
import os, sys,re,glob,bz2
import urllib.request,ssl
import shutil

import logging
import argparse
import datetime

import subprocess
from collections import namedtuple

# Use ralreplicas to extract details from a set of  file paths


parser = argparse.ArgumentParser()
parser.add_argument('-i',"--input",type=str,
                    help='input file with list of full paths')

parser.add_argument('-o',"--output",type=str,
                    help='output file with csv output')


def get_ralreplica(filename):
    response = subprocess.run(['ralreplicas',filename],  stdout=subprocess.PIPE,stderr=subprocess.PIPE )
    if response.returncode != 0:
        print(response)
        raise ValueError(f"Error in ralreplicas: {response.stderr.decode('utf-8')}")
    return response.stdout.decode('utf-8').strip()


def get_nsls(filename):
    response = subprocess.run(['nsls','-T','--checksum',filename],  stdout=subprocess.PIPE,stderr=subprocess.PIPE )
    if response.returncode != 0:
        print(response)
        raise ValueError(f"Error in nsls: {response.stderr.decode('utf-8')}")
    return response.stdout.decode('utf-8').strip()

def get_stager_qry(filename):
    response = subprocess.run(['stager_qry','-M',filename],  stdout=subprocess.PIPE,stderr=subprocess.PIPE )
    if response.returncode != 0:
        print(response)
        raise ValueError(f"Error in get_stager_qry: {response.stderr.decode('utf-8')}")
    return response.stdout.decode('utf-8').strip()


CastorFile = namedtuple("CastorFile","path is_staged is_migrated size tape_pool file_class_id vid extras")

def decode_ralreplica(replica_output):
    print(replica_output)

    rows = replica_output.split('\n')

    nsls = rows[1].strip().split()
    #print(nsls)
    try:
        fsize = nsls[8]
        path  = nsls[-1]
    except Exception as e:
        print(nsls)
        raise(e)

    is_staged = False if "not in stager" in rows[0] else True
    if not is_staged and "DISCOPY_STAGED" in replica_output:
        raise ValueError("Mismatch on staging: ", replica_output)

    is_migrated = True if   nsls[4][0] == 'm' else False

    re_tape_pool = re.search("NAME=([a-zA-Z0-9]+)",replica_output)
    if re_tape_pool is None:
        raise ValueError("Mismatch on tape_pool: ", replica_output)
    tape_pool = re_tape_pool.group(1)

    re_id = re.search("ID=([a-zA-Z0-9]+)",replica_output)
    if re_id is None:
        raise ValueError("Mismatch on ID: ", replica_output)
    file_class_id = re_id.group(1)

    tape_volume_status = re.search("Tape volume status = (.*)\n?",replica_output).group(1).strip().split()

    vid = tape_volume_status[0]

    c = CastorFile(path,is_staged, is_migrated ,fsize,tape_pool,file_class_id,vid,  ':'.join(tape_volume_status)
                    )
    return c


def write_csvline(f, castorObj):
    tmp = ','.join( (castorObj.path ,
        '1' if castorObj.is_staged else "0",
        "1" if castorObj.is_migrated else "0",         
        castorObj.size ,
        castorObj.tape_pool ,
        castorObj.file_class_id ,
        castorObj.vid, 
        castorObj.extras) )
    f.write(tmp+'\n')



def analyse_path(path):
    return decode_ralreplica(get_ralreplica(path))

if __name__ == "__main__":
    args = parser.parse_args()
    today = datetime.datetime.now()

    with open(args.output,'w') as foo:
        with open(args.input, 'r') as fii:
            for p in fii.readlines():
                print(p)
                c = analyse_path(p)
                print(c)
                write_csvline(foo, c)
                





