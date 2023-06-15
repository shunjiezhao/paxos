package paxos

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"log"
	"sync"
	"testing"
)

var (
	key = "test-key"

	smallProposal = &Proposal{
		Id:  &PaxosInstanceId{Key: key},
		Bal: &BallotNum{N: 1, ProposerId: 1},
		V:   &Value{},
	}
	biggestProposal = &Proposal{
		Id:  &PaxosInstanceId{Key: key},
		Bal: &BallotNum{N: 3, ProposerId: 1},
		V:   &Value{Val: "zsj"},
	}
)

var id int

func generateKVServers(num int) (ids []int, server []*KVServer) {
	log.SetFlags(log.Lshortfile)
	for i := 1; i <= num; i++ {
		ids = append(ids, id)
		id++
		server = append(server, &KVServer{
			Storage: map[string]*KeyAcceptor{},
		})
	}

	return
}

func TestAll(t *testing.T) {
	t.Run("TestRunSimple", func(t *testing.T) {
		ids, server := generateKVServers(3)
		_, clean := runAcceptors(ids, server)
		defer clean()

		proposal := &Proposal{
			Id: &PaxosInstanceId{
				Key: "test-key",
			},
			V: nil,
			Bal: &BallotNum{
				N:          1,
				ProposerId: 1,
			},
		}
		value := "123"
		proposal.RunPaxos(ids, &Value{Val: value})              // write
		assert.Equal(t, value, proposal.RunPaxos(ids, nil).Val) // read
	})

	t.Run("TestRunTwoProposal", func(t *testing.T) {
		ids, server := generateKVServers(3)
		_, clean := runAcceptors(ids, server)
		defer clean()
		wg := &sync.WaitGroup{}
		wg.Add(2)
		// biggest should be accept
		go func() {
			defer wg.Done()
			Proposal := proto.Clone(smallProposal).(*Proposal)
			Proposal.RunPaxos(ids, &Value{Val: "1"})
		}()
		go func() {
			defer wg.Done()
			Proposal := proto.Clone(biggestProposal).(*Proposal)
			Proposal.RunPaxos(ids, &Value{Val: "1"})
		}()
		wg.Wait()
		val := biggestProposal.RunPaxos(ids, nil)
		assert.Equal(t, val.Val, biggestProposal.V.Val)

	})

	/*
			 	2c.
				proposal in accept
					1. not have val
					2. last proposal <= this

			    proposal in prepare
					1. don't be accept when have a biggest proposal
					2. will be accpet when acceptor don't have proposal or this is a biggest proposal


		if have a biggest proposal
			client should inc proposal id
		if have a maxVoted take it to phase2
	*/
	t.Run("TestPrepare", func(t *testing.T) {
		t.Run("Acceptor have a biggest proposal, so this proposal can not be accept", func(t *testing.T) {
			ids, server := generateKVServers(3)
			biggestAcceptor := &Acceptor{LastProposal: &BallotNum{ProposerId: 1, N: 2}}
			Proposal := proto.Clone(smallProposal).(*Proposal)

			server[0].Storage[key] = &KeyAcceptor{acceptor: proto.Clone(biggestAcceptor).(*Acceptor)}
			server[1].Storage[key] = &KeyAcceptor{acceptor: proto.Clone(biggestAcceptor).(*Acceptor)}
			_, clean := runAcceptors(ids, server)
			defer clean()
			_, high, err := Proposal.Prepare(ids)
			if assert.Error(t, err) && assert.NotNil(t, high) {
				assert.Equal(t, high.ProposerId, biggestAcceptor.LastProposal.ProposerId)
				assert.Equal(t, high.N, biggestAcceptor.LastProposal.N)
			}
		})

		t.Run("proposal have a biggest proposal, so this proposal can be accept", func(t *testing.T) {
			ids, server := generateKVServers(3)
			biggestAcceptor := &Acceptor{LastProposal: &BallotNum{ProposerId: 1, N: 2}, Val: &Value{Val: "123"}, LastAccept: &BallotNum{ProposerId: 1, N: 2}}
			Proposal := proto.Clone(biggestProposal).(*Proposal)
			server[0].Storage[key] = &KeyAcceptor{acceptor: proto.Clone(biggestAcceptor).(*Acceptor)}
			server[1].Storage[key] = &KeyAcceptor{acceptor: proto.Clone(biggestAcceptor).(*Acceptor)}
			_, clean := runAcceptors(ids, server)
			defer clean()
			val, high, err := Proposal.Prepare(ids)
			assert.Nil(t, err)
			assert.Nil(t, high)
			assert.Equal(t, val.Val, biggestAcceptor.Val.Val)
		})
	})
	t.Run("TestAccept", func(t *testing.T) {
		t.Run("proposal proposal GE acceptor, so this proposal can be accept", func(t *testing.T) {
			ids, server := generateKVServers(3)
			biggestAcceptor := &Acceptor{LastProposal: &BallotNum{ProposerId: 1, N: 2}, Val: &Value{Val: "123"}, LastAccept: &BallotNum{ProposerId: 1, N: 2}}
			Proposal := proto.Clone(biggestProposal).(*Proposal)

			server[0].Storage[key] = &KeyAcceptor{acceptor: proto.Clone(biggestAcceptor).(*Acceptor)}
			server[1].Storage[key] = &KeyAcceptor{acceptor: proto.Clone(biggestAcceptor).(*Acceptor)}
			_, clean := runAcceptors(ids, server)
			defer clean()
			// accept
			bal, err := Proposal.Accept(ids)
			assert.Nil(t, err)
			assert.Nil(t, bal)
			// read latest value
			prepare, _, err := Proposal.Prepare(ids)
			assert.Nil(t, err)
			assert.Equal(t, prepare.Val, Proposal.V.Val)
		})

		t.Run("proposal proposal less then acceptor, so this proposal can not be accept", func(t *testing.T) {
			ids, server := generateKVServers(3)
			biggestAcceptor := &Acceptor{LastProposal: &BallotNum{ProposerId: 1, N: 2}, Val: &Value{Val: "123"}, LastAccept: &BallotNum{ProposerId: 1, N: 2}}
			Proposal := proto.Clone(smallProposal).(*Proposal)
			server[0].Storage[key] = &KeyAcceptor{acceptor: proto.Clone(biggestAcceptor).(*Acceptor)}
			server[1].Storage[key] = &KeyAcceptor{acceptor: proto.Clone(biggestAcceptor).(*Acceptor)}
			_, clean := runAcceptors(ids, server)
			defer clean()
			//  can not accept
			bal, err := Proposal.Accept(ids)
			fmt.Println(bal, err)
			if assert.Error(t, err) && assert.NotNil(t, bal) {
				assert.Equal(t, bal.ProposerId, biggestAcceptor.LastProposal.ProposerId)
				assert.Equal(t, bal.N, biggestAcceptor.LastProposal.N)
			}
		})
	})
}
