# ral-tapestudies
tapestudy commands

### get list of pools
python tapeStudies/tape_pool_getter.py --get_pools '.*atl.*'

### get list of vids for a given pool
python   tapeStudies/tape_pool_getter.py --get_vids atl17
 - or, add additional information to each vid
python   tapeStudies/tape_pool_getter.py --get_vids_details atl17

- combine the above commands for all vids
```
for pool in `python   tapeStudies/tape_pool_getter.py --get_pools '.*atl.*'`; do echo ${pool}; python   tapeStudies/tape_pool_getter.py --get_vids_details ${pool} > tape_data/pool_${pool}.txt ; done 
```

## VID information
### get all files for a given pool split by VID 
export POOL=atl17
for VID in `python   tapeStudies/tape_pool_getter.py --get_vids ${POOL}`; do echo ${VID}; python   tapeStudies/tape_pool_getter.py --get_files_vid ${VID} > tape_data/files_${POOL}_${VID}.txt; done 




