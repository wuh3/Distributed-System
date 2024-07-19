package routines

import (
	"bytes"
	"cs425-mp2/config"
	"cs425-mp2/util"
	"encoding/binary"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	ELECTION_TIMEOUT_MILLI uint = 5000
	VOTE_TIMEOUT_MILLI     uint = 2000
	READ_BUFFER_SIZE       int  = 100

	ELECTION_REQUEST    uint8 = 1
	VOTE                uint8 = 2
	LEADER_NOTIFICATION uint8 = 3
	ROUND_NOTIFICATION  uint8 = 4
)

var LeaderId string = "" // go does not support volatile variable in Java linguo, let's switch for something else later
var localRoundId uint32 = 0
var voterSet map[string]bool = make(map[string]bool)
var endCurrentRound bool = false
var candidateId string = SelfNodeId
var quorumSize int = 100
var electionMessageChan chan *ElectionMessage = make(chan *ElectionMessage)
var leaderElectionPort string

type ElectionMessage struct {
	MessageType uint8
	RoundId     uint32
	NodeId      string
}

func (this *ElectionMessage) ToPayload() []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	uint32Arr := make([]byte, 4)
	buf.WriteByte(this.MessageType)
	binary.LittleEndian.PutUint32(uint32Arr, uint32(this.RoundId))
	buf.Write(uint32Arr)
	buf.Write([]byte(this.NodeId))
	return buf.Bytes()
}

func FromPayload(payload []byte, size int) *ElectionMessage {
	buf := bytes.NewBuffer(payload)
	ret := new(ElectionMessage)
	ret.MessageType = buf.Next(1)[0]
	ret.RoundId = binary.LittleEndian.Uint32(buf.Next(4))
	ret.NodeId = string(buf.Next(size - 5))
	return ret
}

func StartLeaderElectionServer() {
	MEMBERSHIP_SERVER_STARTED.Wait()
	leaderElectionPort = strconv.Itoa(config.LeaderElectionServerPort)
	quorumSize = config.LeaderElectionQuorumSize
	localAddr, err := net.ResolveUDPAddr("udp4", ":"+leaderElectionPort)
	if err != nil {
		log.Fatal("[Leader Election Service] Error resolving udp address", err)
	}

	conn, err := net.ListenUDP("udp4", localAddr)

	if err != nil {
		log.Fatal("[Leader Election Service] Failed to start udp server", err)
	}

	defer conn.Close()

	// start receiving election messages
	buf := make([]byte, READ_BUFFER_SIZE)
	go func() {
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if n > 0 && err == nil {
				msg := FromPayload(buf[:n], n)
				electionMessageChan <- msg
			}
		}
	}()

	LEADER_ELECTION_SERVER_STARTED.Done()

	for { //low priority but needs graceful termination
		for len(LeaderId) == 0 {
			runElection(conn)
		}

		go monitorLeaderFailure()
		go monitorNewElectionRound(conn)

		for len(LeaderId) > 0 {
		}
	}
}

// start whatever service needed to be hosted by a leader
func startLeaderHostedService() {
	FILE_METADATA_SERVER_SIGTERM.Add(1)
	go NewFileMetadataService().Register()
}

func termLeaderHostedService() {
	FILE_METADATA_SERVER_SIGTERM.Done()
}

func runElection(conn *net.UDPConn) {
	localRoundId += 1
	LeaderId = ""
	voterSet = make(map[string]bool)
	endCurrentRound = false
	candidateId = SelfNodeId

	// multicast election request to all alive members
	msg := ElectionMessage{
		MessageType: ELECTION_REQUEST,
		RoundId:     localRoundId,
		NodeId:      SelfNodeId,
	}
	multicast(conn, msg.ToPayload())

	go waitAndVote(conn, localRoundId)

	// start processing inbound election requests and collecting vote
	electionTimeout := time.After(time.Duration(ELECTION_TIMEOUT_MILLI) * time.Millisecond)
	for len(LeaderId) == 0 && !endCurrentRound {
		select {
		case <-electionTimeout:
			// log.Printf("Election timeout for round %d", localRoundId)
			return
		case msg := <-electionMessageChan:
			handleElectionMsg(conn, msg)
		}
	}
}

func handleElectionMsg(conn *net.UDPConn, msg *ElectionMessage) {
	if msg.RoundId < localRoundId {
		// log.Printf("Discarded election message of type %d with round ID %d", msg.MessageType, msg.RoundId)
		return // discard message from previous rounds
	}

	if msg.RoundId > localRoundId {
		log.Printf("Fast-forwarding round")
		localRoundId = msg.RoundId - 1
		endCurrentRound = true // fast-forward to latest election round
		return
	}

	if msg.MessageType == ELECTION_REQUEST {
		// log.Printf("Received election request from node %s for round %d", msg.NodeId, msg.RoundId)
		if msg.NodeId < candidateId {
			candidateId = msg.NodeId
		}
	} else if msg.MessageType == VOTE {
		// log.Printf("Received vote from node %s for round %d", msg.NodeId, msg.RoundId)
		_, exists := voterSet[msg.NodeId]
		if !exists {
			voterSet[msg.NodeId] = true
		}
		if len(voterSet) >= quorumSize {
			LeaderId = SelfNodeId
			// log.Print("Received majority votes, multicasting leader notification")
			msg := ElectionMessage{
				MessageType: LEADER_NOTIFICATION,
				RoundId:     localRoundId,
				NodeId:      SelfNodeId,
			}
			multicast(conn, msg.ToPayload())
		}
	} else if msg.MessageType == LEADER_NOTIFICATION {
		LeaderId = msg.NodeId
		// log.Printf("Elected leader at %s for round %d", LeaderId, localRoundId)
		return
	} 
}

// wait for a while before deciding who to vote
func waitAndVote(conn *net.UDPConn, votingRoundId uint32) {
	<-time.After(time.Duration(VOTE_TIMEOUT_MILLI) * time.Millisecond)
	// we might have moved to another round
	if votingRoundId != localRoundId {
		log.Printf("Abstained vote for round %d ", votingRoundId)
		return
	}
	// log.Printf("Casted vote for %s at round %d ", candidateId, votingRoundId)
	msg := ElectionMessage{
		MessageType: VOTE,
		RoundId:     votingRoundId,
		NodeId:      SelfNodeId,
	}
	addr := NodeIdToIP(candidateId) + ":" + leaderElectionPort
	unicast(conn, addr, msg.ToPayload())
}

func monitorLeaderFailure() {
	for len(LeaderId) > 0 {
		select {
		case event := <-util.MembershipListEventChan:
			if event.IsOffline() && event.NodeId == LeaderId {
				LeaderId = ""
				return
			}
		default:
		}
	}
}

func monitorNewElectionRound(conn *net.UDPConn) {
	for len(LeaderId) > 0 {
		select {
		case msg := <-electionMessageChan:
			// could be new joiner or delayed packet, inform it to sync up round ID anyways
			if msg.RoundId < localRoundId && msg.MessageType == ELECTION_REQUEST {
				outMsg := ElectionMessage{
					MessageType: ROUND_NOTIFICATION,
					RoundId:     localRoundId,
					NodeId:      SelfNodeId,
				}
				addr := NodeIdToIP(msg.NodeId) + ":" + leaderElectionPort
				unicast(conn, addr, outMsg.ToPayload())
				// some node requested new election
			} else if msg.RoundId > localRoundId {
				LeaderId = ""
				localRoundId = msg.RoundId - 1
				return
			}
		default:
		}
	}
}

func unicast(conn *net.UDPConn, addr string, payload []byte) {
	remoteAddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		log.Printf("Error resolving remote address %s\n", addr)
		return
	}
	_, err = conn.WriteToUDP(payload, remoteAddr)
	if err != nil {
		log.Printf("Failed to send udp packet to %s %s", addr, err)
	}
}

func multicast(conn *net.UDPConn, payload []byte) {
	aliveMembers := LocalMembershipList.AliveMembers()

	for _, ip := range aliveMembers {
		addr := ip + ":" + leaderElectionPort
		remoteAddr, err := net.ResolveUDPAddr("udp4", addr)
		if err != nil {
			log.Printf("Error resolving remote address %s\n", addr)
			continue
		}
		_, err = conn.WriteToUDP(payload, remoteAddr)
		if err != nil {
			log.Printf("Failed to send udp packet to %s %s", addr, err)
		}
	}
}

func NodeIdToIP(nodeId string) string {
	splitted := strings.Split(nodeId, ":")
	if len(splitted) != 2 {
		log.Printf("Error parsing node id (%s) to udp address", nodeId)
	}
	return splitted[0]
}
