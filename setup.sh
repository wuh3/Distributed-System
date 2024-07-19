cd ~

mkdir sdfs
mkdir local
touch config.txt

echo "MEMBERSHIP_SERVICE_PORT=8001" > config.txt
echo "MEMBERSHIP_PROTOCOL=G" >> config.txt
echo "IS_INTRODUCER=TRUE" >> config.txt
echo "INTRODUCER_IP=fa23-cs425-7401.cs.illinois.edu" >> config.txt
echo "INTRODUCER_PORT=8002" >> config.txt


echo "LEADER_ELECTION_SERVER_PORT=8003" >> config.txt
# 6
echo "LEADER_ELECTION_QUORUM_SIZE=5" >> config.txt


echo "REPLICATION_FACTOR=4" >> config.txt

echo "RPC_SERVER_PORT=8004" >> config.txt
echo "FILE_SERVER_RECEIVE_PORT=8005" >> config.txt
echo "DFS_CLIENT_RECEIVE_PORT=8006" >> config.txt

echo "LOG_FILE_NAME=log" >> config.txt
echo "LOG_SERVER_ID=vm$1" >> config.txt
echo "SERVER_HOSTNAMES=fa23-cs425-7401.cs.illinois.edu,fa23-cs425-7402.cs.illinois.edu,fa23-cs425-7403.cs.illinois.edu,fa23-cs425-7404.cs.illinois.edu,fa23-cs425-7405.cs.illinois.edu,fa23-cs425-7406.cs.illinois.edu,fa23-cs425-7407.cs.illinois.edu,fa23-cs425-7408.cs.illinois.edu,fa23-cs425-7409.cs.illinois.edu,fa23-cs425-7410.cs.illinois.edu" >> config.txt
echo Set up complete.