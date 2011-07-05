#!/bin/bash

# read tables back in with
# bcp <table-name> in <table-file> -U aca_ops -S sybase -c -t '|' -Y
# if needed

# source /soft/SYBASE_OCS15/SYBASE.csh or .sh to get bcp
source /soft/SYBASE_OCS15/SYBASE.sh
now=$(date +"%Y-%m-%d") 

for table in load_segments timelines cmds cmd_intpars cmd_fltpars cmd_states tl_built_loads tl_processing tl_obsids
do 
    bcp ${table} in ${table}_${now}.bcp -U aca_test -S sybase -P '400-ZET' -c -t '|' -Y
done




