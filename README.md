# Instructions
## Running the program
1. Install Go
2. Clone the repo and cd into project folder: `cd cs-425-mp-3`
3. Run the setup script with the following command (server id can be any number and is used only for easy identification when retrieving logs from remote servers)

   `sh setup.sh <server-id>`

   **Please make sure there isn't a folder named `log` under your home directory.**

4. Run the program with `go run main.go`
5. The program will ask you whether it should be started as a bootstrap server. 
    
   **Starting the program as a bootstrap server means the current vm will also act as an introducer.**
7. Follow the prompts to finish starting up services



## Please Note
1. By default the log server (used for retrieving logs from remote machines) will run on port 8000. To use a different port, update the `LOG_SERVER_PORT=port` entry in /UserHome/config.txt
2. Program log file is by default named `log` (configured in config.txt) and stored under the home directory. When the program starts, it will truncate `log` (overwriting logs from previous run).
3. Please make sure IP address of introducer is properly configured for the membership service

