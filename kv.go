package paxos

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
	"log"
	"net"
	"sync"
)

func (a *BallotNum) GE(b *BallotNum) bool {
	if a.N != b.N {
		return a.N > b.N
	}
	return a.ProposerId >= b.ProposerId
}

func (p *Proposal) Prepare(acceptorIds []int, quorum int) (*Value, *BallotNum, error) {
	all := p.rpcToAll(acceptorIds, "prepare", p)
	successCnt := 0
	highBal := *p.Bal
	maxVoted := &Acceptor{LastAccept: &BallotNum{}}

	for _, r := range all {
		if r == nil {
			continue
		}
		if !p.Bal.GE(r.LastProposal) {
			if r.LastProposal.GE(&highBal) {
				highBal = *r.LastProposal
			}
			continue
		}

		if r.LastAccept.GE(maxVoted.LastAccept) { // if get a biggest accepted proposal, then we will catch up it
			maxVoted = r
		}
		successCnt++
		if successCnt == quorum {
			return maxVoted.Val, nil, nil
		}
	}
	return nil, &highBal, errors.New("not enough quorum")
}

func (p *Proposal) Accept(acceptorIds []int, quorum int) (*BallotNum, error) {
	all := p.rpcToAll(acceptorIds, "accept", p)
	successCnt := 0
	highBal := *p.Bal

	for _, r := range all {
		if r == nil {
			continue
		}
		if !p.Bal.GE(r.LastProposal) {
			if r.LastProposal.GE(&highBal) {
				highBal = *r.LastProposal
			}
			continue
		}

		successCnt++
		if successCnt == quorum {
			return nil, nil
		}
	}
	return &highBal, errors.New("not enough quorum")
}

func getAcceptorAddress(id int) string {
	return fmt.Sprintf("127.0.0.1:%d", 5000+id)
}

func (p *Proposal) rpcToAll(ids []int, opName string, args *Proposal) []*Acceptor {
	log.Println("rpcToAll method: ", opName)
	ans := make([]*Acceptor, len(ids))
	for i, id := range ids {
		address := getAcceptorAddress(id)
		conn, err := grpc.Dial(address,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal("dial fail", err)
		}
		defer conn.Close()
		client := NewPaxosKvClient(conn)
		var acceptor *Acceptor
		if opName == "prepare" {
			acceptor, err = client.Prepare(context.Background(), args)
		} else {
			acceptor, err = client.Prepare(context.Background(), args)
		}
		if err != nil {
			log.Printf("Proposer: %+v fail from A(%d) : %v", opName, id, err)
		}
		log.Printf("Proposer: recv %s reply from A(%d): %v", opName, id, acceptor)
		ans[i] = acceptor
	}
	return ans
}

type KeyAcceptor struct {
	mu       sync.Mutex
	acceptor Acceptor
}

type KVServer struct {
	UnimplementedPaxosKvServer
	mu      sync.Mutex
	Storage map[string]*KeyAcceptor
}

func (s *KVServer) Prepare(ctx context.Context, proposal *Proposal) (*Acceptor, error) {
	log.Printf("[KvServer]: prepare: %+v", proposal)
	v := s.getKeyAcceptorL(proposal.Id)
	defer v.mu.Unlock()

	reply := v.acceptor // get this version accepted proposal in this acceptor
	if proposal.Bal.GE(reply.LastProposal) {
		v.acceptor.LastProposal = proposal.Bal // biggest proposal
		// and we should not allow small proposal be accepted
	}
	return &reply, nil // will return lastProposal if have
}

func (s *KVServer) Accept(ctx context.Context, proposal *Proposal) (*Acceptor, error) {
	v := s.getKeyAcceptorL(proposal.Id)
	defer v.mu.Unlock()

	reply := &Acceptor{
		LastProposal: proto.Clone(v.acceptor.LastProposal).(*BallotNum), // return last proposal
	}

	if proposal.Bal.GE(v.acceptor.LastProposal) { // proposal.Bal = acceptor.LastProposal
		// accept proposal
		v.acceptor.LastProposal = proposal.Bal // maybe we don't allow this but other acceptor promise
		v.acceptor.Val = proposal.V
		v.acceptor.LastAccept = proposal.Bal
	}
	return reply, nil
}

func (s *KVServer) getKeyAcceptorL(id *PaxosInstanceId) *KeyAcceptor {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := id.Key
	record, ok := s.Storage[key]
	if !ok { // not have this key
		s.Storage[key] = &KeyAcceptor{
			acceptor: Acceptor{
				LastAccept:   &BallotNum{},
				LastProposal: &BallotNum{},
			},
		}
		record = s.Storage[key]
	}
	record.mu.Lock()
	return record
}

func runAcceptors(ids []int) (kvs []*grpc.Server) {
	for _, id := range ids {
		listen, err := net.Listen("tcp", getAcceptorAddress(id))
		if err != nil {
			panic(err)
		}
		log.Println("run server in address", getAcceptorAddress(id))
		server := grpc.NewServer()
		RegisterPaxosKvServer(server, &KVServer{
			Storage: map[string]*KeyAcceptor{},
		})
		reflection.Register(server)

		go server.Serve(listen)
		kvs = append(kvs, server)
	}
	return
}
