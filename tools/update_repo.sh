#!/bin/bash

for i in {1..9}
do
    echo VM$i
    ssh haozhew3@fa23-cs425-740$i.cs.illinois.edu "cd ~/cs425/; git stash; git checkout qa2;  git pull origin qa2 --rebase; exit"
done
echo VM10
ssh haozhew3@fa23-cs425-7410.cs.illinois.edu "cd ~/cs425/; git stash; git checkout qa2; git pull origin qa2 --rebase; exit"
echo 'Repo Update complete.'