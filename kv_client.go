package paxos

func (p *Proposal) RunPaxos(acceptorIds []int, val *Value) *Value {

	for {
		p.V = val

		maxVotedVal, higherBal, err := p.Prepare(acceptorIds)

		if err != nil {
			p.Bal.N = higherBal.N + 1
			continue
		}

		if maxVotedVal != nil {
			p.V = maxVotedVal
		}

		// val == nil 是一个读操作,
		// 没有读到voted值说明之前没有值，返回即可
		if p.V == nil {
			return nil
		}

		higherBal, err = p.Accept(acceptorIds)

		if err != nil { // 当发现自己不能通过时，尝试更高的
			p.Bal.N = higherBal.N + 1
			continue
		}

		return p.V
	}
}
