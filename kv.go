package paxos

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"log"
	"sync"
	"time"
)

func (a *BallotNum) GE(b *BallotNum) bool {
	if a.N != b.N {
		return a.N > b.N
	}
	return a.ProposerId >= b.ProposerId
}

func (p *Proposal) Phase1(acceptorIds []int, quorum int) (*Value, *BallotNum, error) {

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)

	defer cancelFunc()
	all := p.rpcToAll(ctx, acceptorIds, "prepare", func(client PaxosClient) (*Acceptor, error) {
		return client.Prepare(ctx, p)
	})
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

func (p *Proposal) Phase2(acceptorIds []int, quorum int) (*BallotNum, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)

	defer cancelFunc()
	all := p.rpcToAll(ctx, acceptorIds, "accept", func(client PaxosClient) (*Acceptor, error) {
		return client.Accept(ctx, p)
	})
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

func (p *Proposal) rpcToAll(ctx context.Context, ids []int, opName string, op func(client PaxosClient) (*Acceptor, error)) []*Acceptor {
	ans := make([]*Acceptor, len(ids))
	wg := &sync.WaitGroup{}
	for i, id := range ids {
		go func(i, id int) {
			defer wg.Done()
			address := fmt.Sprintf("127.0.0.1:%d", 50000+int64(id))
			conn, err := grpc.DialContext(ctx, address,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatal("dial fail", err)
			}
			defer conn.Close()
			acceptor, err := op(NewPaxosClient(conn))
			if err != nil {
				log.Printf("Proposer: %+v fail from A(%d) : %v", opName, id, err)
			}
			log.Printf("Proposer: recv %s reply from A(%d): %v", opName, id, acceptor)
			ans[i] = acceptor

		}(i, id)
	}
	wg.Wait()
	return ans
}

type KeyAcceptor struct {
	mu       sync.Mutex
	acceptor Acceptor
}

type KVServer struct {
	UnimplementedPaxosServer
	mu      sync.Mutex
	Storage map[string]*KeyAcceptor
}

func (s *KVServer) Prepare(ctx context.Context, proposal *Proposal) (*Acceptor, error) {
	v := s.getLockVersion(proposal.Id)
	defer v.mu.Lock()

	reply := v.acceptor // get this version accepted proposal in this acceptor
	if proposal.Bal.GE(reply.LastProposal) {
		v.acceptor.LastProposal = proposal.Bal // biggest proposal
		// and we should not allow small proposal be accepted
	}
	return &reply, nil // will return lastProposal if have
}

func (s *KVServer) Accept(ctx context.Context, proposal *Proposal) (*Acceptor, error) {
	v := s.getLockVersion(proposal.Id)
	defer v.mu.Lock()

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

func (s *KVServer) getLockVersion(id *PaxosInstanceId) *KeyAcceptor {
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
