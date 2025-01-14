package masterproto

type RegisterArgs struct {
	Addr string
	Port int
	Id   int32
}

type RegisterReply struct {
	ReplicaId int
	NodeList  []string
	Ready     bool
	IsLeader  bool
}

type GetLeaderArgs struct {
}

type GetLeaderReply struct {
	LeaderId int
}

type GetReplicaListArgs struct {
}

type GetReplicaListReply struct {
	ReplicaList []string
	AliveList   []bool
	Ready       bool
}
