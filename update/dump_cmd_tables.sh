#!/bin/bash

# read tables back in with
# bcp <table-name> in <table-file> -U aca_ops -S sybase -c -t '|' -Y
# if needed

# source /soft/SYBASE_OCS15/SYBASE.csh or .sh to get bcp
#source /soft/SYBASE_OCS15/SYBASE.sh
now=$(date +"%Y-%m-%d") 

for table in load_segments timelines cmds cmd_intpars cmd_fltpars cmd_states mp_load_info tl_built_loads tl_processing tl_obsids
do 
    echo "copying out $table to ${table}_${now}.bcp "
    # and I just stuck the read-only password in here because
    # I didn't feel like writing an app to read the password file
    #bcp ${table} out ${table}_${now}.bcp -U aca_read -S sybase -P 'CRC-32B' -c -t '|' -Y
    # using sqsh to get longer format numbers, trimming leading space with sed 
    sqsh -U aca_read -S sybase -P 'CRC-32B' -C "select * from ${table}" -s '|' -m bcp -w 256 -L float=20.12 -L bcp_rowsep="" \
        | sed -r 's/\| */|/g' > ${table}_${now}.bcp
    
done




