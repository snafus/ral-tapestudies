import os, sys,re,glob,bz2
import urllib.request,ssl
import shutil

import logging
import argparse
import datetime
import json

import subprocess
from collections import namedtuple, defaultdict
import numpy as np

#Some utility scripts and cli tools to get and display:
# List of pools
# VIDs within pools
# files within VIDs
# Summary information of the above


#based on https://stackoverflow.com/questions/42865724/python-parse-human-readable-filesizes-into-bytes/42865957 
units = {"B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40,
                  "KB": 10**3, "MB" : 10**6, "GB" : 10**9, "TB": 10**12}
def parse_unit(unit_str):
    global units
    return units[unit_str.strip()]

def parse_value(value_st):
    """Take a string of number(space)unit, and return the bytes size
    """
    r = re.match('([0-9\.+\-]+).*?([a-zA-Z]+)',value_st)
    unit = parse_unit(r.group(2))
    value = float(r.group(1))
    return unit * value




def run_command(cmd, arguments = None):
    """Run shell command, cmd, with arguments
    """
    cmd = [cmd] 
    if arguments is not None:
        cmd = cmd + arguments
    response = subprocess.run(cmd,  stdout=subprocess.PIPE,stderr=subprocess.PIPE )
    if response.returncode != 0:
        print(response)
        raise ValueError(f"Error running command {cmd} {arguments}: {response.stderr.decode('utf-8')}")
    return response.stdout.decode('utf-8').strip()


def get_files_vid(vid_name):
    """Return the response from nslisttape -V for a single VID
    """
    cmd = "nslisttape"
    args = ['-V',vid_name]
    return run_command(cmd,args).split('\n')

def get_tape_pools(tape_pool_pattern=".*"):
    """Return a list of tape pools, filtered by the  regex expression
    """
    tape_pools = []
    cmd = "printtapepool"
    output = run_command(cmd)
    for row in output.split('\n'):
        r = row.split()
        if len(r) == 0:
            continue
        g  = re.match(f'({tape_pool_pattern})',r[0])
        if g is None:
            continue
        tape_pools.append(g.group(1))
    return tape_pools

def get_pool_vids_details(tape_pool):
    """For a given pool name, get the associated vids and details from vmgrlisttape
    Returns a list of vids, or an empty list if none
    """
    cmd = 'vmgrlisttape'
    args = ['-P', tape_pool]
    output = run_command(cmd, args)
    return output.split('\n')

def get_pool_vids_names(tape_pool):
    """For a given pool name, get the associated vids names only
    Returns a sorted list of vids, or an empty list if none
    """
    details = get_pool_vids_details(tape_pool)
    return sorted([x.split()[0] for x in details])



def summarise_pool(pool_name):
    """For a give pool, print out some metrics 
    """
    vid_details = get_pool_vids_details(pool_name)
    n_vids = len(vid_details)

    if args.verbose:
        print('\n'.join(vid_details))


    print(f'Pool {pool_name} : contains {n_vids} VIDs')

def summarise_vid(vid_name):
    """ Print out some summary info on Tapes 
    """ 
    rows = get_files_vid(vid_name)
    fsizes = []
    dsets  = set()

    dset_sizes = defaultdict(list)
    dset_total_sizes = defaultdict(float)

    for r in rows:
        l = r.split()
        fsizes .append( int(l[6]))
        name = l[-1]
        ds = name.split('/')[-2]
        dsets.update([ds])
        dset_sizes[ds].append(int(l[6]) /10**9)
        
    fsizes = np.array(fsizes)/10**9
    total_fsize = np.sum(fsizes)
    median, mean, std = np.median(fsizes), np.mean(fsizes), np.std(fsizes)
    print(f'VID: {vid_name}, {len(fsizes)} files')
    print(f'\tTotal file size: {total_fsize:.2f} GB')
    print(f'\tMean/std Median filesize: {mean:.2f} {std:.2f}, {median:.2f} GB')
    
    sizes  = []
    counts = []
    for k,v in dset_sizes.items():
        counts.append(len(dset_sizes[k]))
        sizes.append(np.sum(dset_sizes[k]))
    sizes  = np.array(sizes)
    counts = np.array(counts)
    if args.verbose:
        for k,v in dset_sizes.items():
            print(f'\t{k}: {len(v)} files, size: {np.sum(v):.2f} GB')
    print(f'Datasets: {len(dsets)}')
    print(f'Size on VID: min/max/mean/std:    {np.min(sizes):.2f}, {np.max(sizes):.2f}, {np.mean(sizes):.2f}, {np.std(sizes):.2f} GB')
    print(f'Nfiles on VID: min/max/mean/std:  {np.min(counts)}, {np.max(counts)}, {np.mean(counts):.2f}, {np.std(counts):.2f} GB')


def parse_vid_stats(row):
    keys = ['vid',  # volume visual identifier
        #'side',      # optional side number
        'vsn',      # magnetically recorded volume serial number of the tape
        'library',   # name of the tape library
        'density',   # alphanumeric density
        'lbltype',   # label type
        'model',     # model of cartridge
        'media_letter',  # media identification letter. For example "A", "B" or "C" for SD3 (Redwood), "J" for IBM 3590, "R" for STK 9840
        #'manufacturer',  #
        #'sn',        # cartridge serial number
        'poolname',  #
        'etime',       # indicates the time the volume was entered in the Volume Manager
        'free_space',  #
        'nbfiles',     # number of files written on the tape
        'rcount',      # number of times the volume has been mounted for read
        'wcount',      # number of times the volume has been mounted for write
        'rhost',       # indicates the last tape server where the tape was mounted for read
        'whost',       # indicates the last tape server where the tape was mounted for write
        'rjid',        # is the job id of the last read request
        'wjid',        # is the job id of the last write request
        'rtime',       # indicates the last time the volume was mounted for read
        'wtime',       # indicates the last time the volume was mounted for write
        'status',      # can be FULL, BUSY, RDONLY, ARCHIVED, EXPORTED or DISABLED (or empty)
        ]
    s_out = row.strip().split()
    bad_status = False
    if len(keys) > len(s_out):
        keys.remove('status')
        bad_status = True
    if len(s_out) != len(keys):
        raise RuntimeError("Mismatch between key and output length: ",len(s_out),len(keys),"\\\n",keys,'\\\n',s_out)
    res = {k:v  for k,v in zip(keys,s_out)}
    if bad_status:
        res['status'] = 'N/A'
    return res


def vid_stats(vid_name,pool_name = None):
    """Use vmgrlisttape to retrieve extended data from particular vid
        Optionally provide a pool name
        returns a dict of values
    """
    cmd = 'vmgrlisttape'
    args = ['-s','-x','-V', vid_name]
    if pool_name is not None:
        args += ['-P',pool_name]

    output = run_command(cmd, args)
    return parse_vid_stats(output)

def pool_vid_stats(pool_name):
    cmd = 'vmgrlisttape'
    args = ['-s','-x','-P',pool_name]

    output = run_command(cmd, args)
    r = [parse_vid_stats(row.strip()) for row in output.split('\n')]
    now = datetime.datetime.now().isoformat()
    pool_name = pool_name
    return {'time':now, 'pool':pool_name,'vids':r}



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--get_pools",type=str,default=None,
                    help='Get list of pools, matching a string, if no string, then show all')

    parser.add_argument("--pool_summary",type=str,default=None,
                    help='Sumarise a pool')

    parser.add_argument("--get_vids",type=str,default=None,
                    help='Get vids for a given pool')

    parser.add_argument("--get_vids_details",type=str,default=None,
                    help='Get vid and details for a given pool')

    parser.add_argument("--get_files_vid",type=str,default=None,
                    help='Get files and for a given VID')

    parser.add_argument("--vid_summary",type=str,default=None,
                    help='Sumarise a VID')
    parser.add_argument("-V","--verbose",default=False,action='store_true',
                    help="Make verbose")


    parser.add_argument("--vid_stats",default=False,action='store_true',
                    help='extended stats for vid')
    parser.add_argument("--pool_vid_stats",default=False,action='store_true',
                    help='extended stats for pool')

    parser.add_argument('-p',"--pool",type=str,default=None,
                    help='pool name')
    parser.add_argument('-v',"--vid",type=str,default=None,
                    help='vid name')


    args = parser.parse_args()

    if args.get_pools:
        print('\n'.join(get_tape_pools(args.get_pools)))
    if args.pool_summary:
        summarise_pool(args.pool_summary)

    if args.get_vids:
        print('\n'.join(get_pool_vids_names(args.get_vids)))

    if args.get_vids_details:
        print('\n'.join(get_pool_vids_details(args.get_vids_details)))

    if args.get_files_vid:
        print('\n'.join(get_files_vid(args.get_files_vid)))


    if args.vid_summary:
        summarise_vid(args.vid_summary)
    if args.vid_stats:
        print( vid_stats(args.vid,args.pool) )
    if args.pool_vid_stats:
        print( json.dumps(pool_vid_stats(args.pool)) )

