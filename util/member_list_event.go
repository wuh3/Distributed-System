package util


const (
	EVENT_JOIN int = 0
	EVENT_OFFLINE int = 1
)

type MembershipListEvent struct {
	eventType int
	NodeId string
}


func (this *MembershipListEvent) IsNewJoin() bool {
	return this.eventType == EVENT_JOIN;
}

func (this *MembershipListEvent) IsOffline() bool {
	return this.eventType == EVENT_OFFLINE;
}

func NotifyOffline(e *MemberListEntry){
	event := &MembershipListEvent{
		eventType: EVENT_OFFLINE,
		NodeId: e.NodeId(),
	}
	MembershipListEventChan <- event
}

func NotifyJoin(e *MemberListEntry){
	event := &MembershipListEvent{
		eventType: EVENT_JOIN,
		NodeId: e.NodeId(),
	}
	MembershipListEventChan <- event
}