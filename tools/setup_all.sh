#!/bin/bash

for i in {1..9}
do
    ssh haozhew3@fa23-cs425-740$i.cs.illinois.edu "cd ~/cs425/mp4/; sh setup_local.sh $i"
    echo Node:VM$i set up done...
done
ssh haozhew3@fa23-cs425-7410.cs.illinois.edu "cd ~/cs425/mp4/; sh setup_local.sh 10"
echo Node:VM10 set up done...