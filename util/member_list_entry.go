package util

import (
	"fmt"
	"time"
)

type MemberListEntry struct {
	// first three fields compose the node id
	Ip        [4]uint8
	Port      uint16
	StartUpTs int64 

	SeqNum uint32
	Status uint8

	// fields below are never serialized and sent out
	ExpirationTs int64 // hearbeat must be received before this ts
}

func (this *MemberListEntry) isFailed() bool {
	// ExpirationTs == 0 means it's self entry
	return this.Status == FAILED ||
		(this.Status != LEFT && time.Now().UnixMilli() >= this.ExpirationTs && this.ExpirationTs != 0)
}

func (this *MemberListEntry) isAlive() bool {
	return this.Status != LEFT &&
		this.Status != FAILED &&
		time.Now().UnixMilli() < this.ExpirationTs
}

// a failed/left entry that passed cleanup ts
func (this *MemberListEntry) isObsolete() bool {
	return (this.Status == FAILED || this.Status == LEFT) &&
		this.ExpirationTs <= time.Now().UnixMilli()
}

func (this *MemberListEntry) Addr() string {
	return fmt.Sprintf("%d.%d.%d.%d:%d",
		this.Ip[0], this.Ip[1], this.Ip[2], this.Ip[3], this.Port)
}

func (this *MemberListEntry) IpString() string {
	return fmt.Sprintf("%d.%d.%d.%d",
		this.Ip[0], this.Ip[1], this.Ip[2], this.Ip[3])
}

func (this *MemberListEntry) NodeId() string {
	return fmt.Sprintf("%d.%d.%d.%d:%d-%d",
		this.Ip[0], this.Ip[1], this.Ip[2], this.Ip[3], this.Port, this.StartUpTs)
}

func (this *MemberListEntry) resetTimer() {
	this.ExpirationTs = time.Now().UnixMilli() + TIMEOUT_MILLI
}

func (this *MemberListEntry) setCleanupTimer() {
	this.ExpirationTs = time.Now().UnixMilli() + CLEANUP_MILLI
}

func (this *MemberListEntry) ToString() string {
	format := "id: %d.%d.%d.%d:%d-%d\n" +
		"seqNum: %d\n" +
		"status: %s\n" +
		"expirationTs: %d\n"

	status := "Unknown"
	if this.isFailed() { // in case the flag is not set by a merge yet
		status = "Failed"
	} else {
		switch this.Status {
		case NORMAL:
			status = "Normal"
		case SUS:
			status = "Suspicious"
		case LEFT:
			status = "Left"
		}
	}
	return fmt.Sprintf(format,
		this.Ip[0], this.Ip[1], this.Ip[2], this.Ip[3],
		this.Port,
		this.StartUpTs,
		this.SeqNum,
		status,
		this.ExpirationTs)
}

// merge two entry with the same node id
func (this *MemberListEntry) Merge(remote *MemberListEntry, protocol uint8) *MemberListEntry {

	if remote.Status > this.Status { // LEFT > FAILED > SUS > NORMAL
		this.Status = remote.Status
		if remote.Status == SUS {
			this.resetTimer()
		} else if remote.Status != NORMAL {
			this.setCleanupTimer() // set up cleanup ts for failed/left entries
		}
		reportStatusUpdate(this)
	} else if remote.SeqNum > this.SeqNum &&
		// normal seq num inc
		((remote.Status == NORMAL && this.Status == NORMAL) ||
			// a node revives
			(protocol == GS && remote.Status == NORMAL &&
				this.Status == SUS)) {
		this.Status = NORMAL
		this.SeqNum = remote.SeqNum
		this.resetTimer()
	}

	return this
}

// compare entry node id
func EntryCmp(e1 *MemberListEntry, e2 *MemberListEntry) int {
	for i := 0; i < 4; i++ {
		ipCmp := int(e1.Ip[i]) - int(e2.Ip[i])
		if ipCmp != 0 {
			return ipCmp
		}
	}
	portCmp := int(e1.Port) - int(e2.Port)
	if portCmp != 0 {
		return portCmp
	}
	tsCmp := e1.StartUpTs - e2.StartUpTs
	if tsCmp > 0 {
		return 1
	} else if tsCmp < 0 {
		return -1
	}
	return 0
}
