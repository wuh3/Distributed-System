package routines

import (
	"sync"
	"cs425-mp2/config"
)


var NeedTermination bool

// Termination signals
var SIGTERM sync.WaitGroup     // main termination
var HEARTBEAT_SENDER_TERM sync.WaitGroup // hearbeat sender termination
var FILE_METADATA_SERVER_SIGTERM sync.WaitGroup


// Start up signals
var MEMBERSHIP_SERVER_STARTED sync.WaitGroup
var INTRODUCER_SERVER_STARTED sync.WaitGroup
var LEADER_ELECTION_SERVER_STARTED sync.WaitGroup



func InitSignals() {
	NeedTermination = false
	SIGTERM.Add(1)
	HEARTBEAT_SENDER_TERM.Add(1)

	MEMBERSHIP_SERVER_STARTED.Add(1)
	INTRODUCER_SERVER_STARTED.Add(1)

	LEADER_ELECTION_SERVER_STARTED.Add(1)
}

func WaitAllServerStart(){
	MEMBERSHIP_SERVER_STARTED.Wait()
	LEADER_ELECTION_SERVER_STARTED.Wait()
	// FILE_SERVER_STARTED.Wait()

	if config.IsIntroducer {
		INTRODUCER_SERVER_STARTED.Wait()
	}
}

func SignalTermination() {
	NeedTermination = true 
	SIGTERM.Done()
}
