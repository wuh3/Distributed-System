#!/bin/bash

for i in {1..9}
do
    echo VM$i
    ssh haozhew3@fa23-cs425-740$i.cs.illinois.edu "cd ~/cs425/mp2/tools/; chmod +x node_run_local.sh; chmod +x introducer_run_local.sh; chmod +x update_repo.sh; exit"
done
ssh haozhew3@fa23-cs425-7410.cs.illinois.edu "cd ~/cs425/mp2/tools/; chmod +x node_run_local.sh; chmod +x introducer_run_local.sh; chmod +x update_repo.sh; exit"
echo 'Grant permission complete.'