#!/bin/bash

for i in {1..9}
do
    echo VM$i
    ssh haozhew3@fa23-cs425-740$i.cs.illinois.edu "cd ~/cs425/; git stash; git checkout main; git fetch origin; git checkout qa2; exit"
done
ssh haozhew3@fa23-cs425-7410.cs.illinois.edu "cd ~/cs425/; git stash; git checkout main; git fetch origin; git checkout qa2; exit"
echo 'Branch Update complete.'