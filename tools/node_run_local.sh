#!/bin/bash

cd ~/cs425/mp4
pkill -f main.go
go run main.go
echo Node is on.