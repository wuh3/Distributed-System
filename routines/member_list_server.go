package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"encoding/binary"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"
)

const (
	CONTACT_NUM       int = 3 // #alive members to call per period
	MAX_BOOSTRAP_RETY int = 5
)

var ReceiverDropRate float64 = 0
var SelfNodeId string = ""
var LocalMembershipList *util.MemberList

var membershipServicePort string

func InitLocalMembershipList(){
	LocalMembershipList = util.NewMemberList(uint16(config.MembershipServicePort))
}

func StartMembershipListServer() {
	membershipServicePort = strconv.Itoa(config.MembershipServicePort)

	SelfNodeId = LocalMembershipList.GetSelfNodeId()


	log.Printf("Machine ID: %s", SelfNodeId)

	localAddr, err := net.ResolveUDPAddr("udp4", LocalMembershipList.SelfEntry.Addr())
	if err != nil {
		log.Fatal("Error resolving udp address", err)
	}

	conn, err := net.ListenUDP("udp4", localAddr)

	if err != nil {
		log.Fatal("Failed to start udp server", err)
	}

	// bootstrap if not introducer
	if !config.IsIntroducer {
		introducerAddr := config.IntroducerIp + ":" + strconv.Itoa(config.IntroducerPort)
		boostrapMemberList := getBootstrapMemberList(introducerAddr, LocalMembershipList.Entries.Value.StartUpTs, conn)
		if boostrapMemberList == nil {
			log.Fatal("Membership list server failed to boostrap. Please check introducer address")
		}

		LocalMembershipList.Merge(boostrapMemberList)
	}

	defer conn.Close()

	go startHeartbeatReciever(LocalMembershipList, conn)

	go startHeartbeatSender(LocalMembershipList, conn)

	MEMBERSHIP_SERVER_STARTED.Done()
	SIGTERM.Wait()
}

func startHeartbeatReciever(localList *util.MemberList, conn *net.UDPConn) {

	buf := make([]byte, util.MAX_ENTRY_NUM*util.ENTRY_SIZE+5)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		// artificial packet drop
		if ReceiverDropRate != 0 {
			if rand.Float64() <= ReceiverDropRate {
				continue
			}
		}
		if err == nil && n > 0 {
			remoteList := util.FromPayload(buf, n)
			if remoteList != nil {
				localList.Merge(remoteList)
			}
		}
	}
}

func startHeartbeatSender(localList *util.MemberList, conn *net.UDPConn) {

	for {
		time.Sleep(time.Duration(util.PERIOD_MILLI) * time.Millisecond)
		localList.IncSelfSeqNum()

		// read the system termination flag
		needTermination := NeedTermination
		if needTermination {
			localList.SelfEntry.Status = util.LEFT
		}

		payloads := localList.ToPayloads()
		aliveMembers := localList.AliveMembers()

		// randomly choose members to contact
		if len(aliveMembers) > CONTACT_NUM {
			rand.NewSource(time.Now().UnixNano())
			rand.Shuffle(len(aliveMembers), func(i, j int) {
				aliveMembers[i], aliveMembers[j] = aliveMembers[j], aliveMembers[i]
			})
			aliveMembers = aliveMembers[:CONTACT_NUM]
		}

		for _, ip := range aliveMembers {
			addr := ip + ":" + membershipServicePort
			remoteAddr, err := net.ResolveUDPAddr("udp4", addr)
			if err != nil {
				log.Printf("Error resolving remote address %s\n", addr)
				continue
			}

			for i, payload := range payloads {
				_, err := conn.WriteToUDP(payload, remoteAddr)
				if err != nil {
					log.Printf("Failed to send  member list %d/%d to %s  %s", i+1, len(payloads), addr, err)
				}
			}
		}

		if needTermination {
			conn.Close()
			HEARTBEAT_SENDER_TERM.Done()
			return
		}
	}

}

func getBootstrapMemberList(introducerAddr string, startUpTs int64, conn *net.UDPConn) *util.MemberList {

	addr, err := net.ResolveUDPAddr("udp4", introducerAddr)

	if err != nil {
		log.Fatal("Failed to resolve boostrap server address", err)
	}

	buf := make([]byte, util.MAX_ENTRY_NUM*util.ENTRY_SIZE+5)
	tsBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBuf, uint64(startUpTs))

	for i := 0; i < MAX_BOOSTRAP_RETY; i++ {

		// send join request and advertise startup ts as part of node id
		conn.WriteToUDP(append([]byte("JOIN"), tsBuf...), addr)
		// timeout if nothing is received after 3 seconds
		timeout := time.After(3 * time.Second)
		readRes := make(chan int)
		go func() {
			n, _, err := conn.ReadFromUDP(buf)
			if n > 0 && err == nil {
				readRes <- n
			}
		}()
		select {
		case <-timeout:
			log.Printf("Error retrieving bootstrap member list, attempt %d/%d", i+1, MAX_BOOSTRAP_RETY)
		case n := <-readRes:
			return util.FromPayload(buf, n)
		}

	}
	return nil
}
