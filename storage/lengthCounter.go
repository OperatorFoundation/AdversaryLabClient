package storage

import (
	"fmt"
	"strconv"

	riak "github.com/basho/riak-go-client"
)

type LengthCounter struct {
	path    string
	cluster *riak.Cluster
	first   bool
}

func startRiak() *riak.Cluster {
	riak.EnableDebugLogging = true

	nodeOpts := &riak.NodeOptions{
		RemoteAddress: "localhost:8087",
	}

	var node *riak.Node
	var err error
	if node, err = riak.NewNode(nodeOpts); err != nil {
		fmt.Println(err.Error())
	}

	nodes := []*riak.Node{node}
	opts := &riak.ClusterOptions{
		Nodes: nodes,
	}

	cluster, err := riak.NewCluster(opts)
	if err != nil {
		fmt.Println(err.Error())
	}

	if err = cluster.Start(); err != nil {
		fmt.Println(err.Error())
	}

	// ping
	ping := &riak.PingCommand{}
	if err = cluster.Execute(ping); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("ping passed")
	}

	return cluster
}

func NewLengthCounter(name string) (*LengthCounter, error) {
	cluster := startRiak()

	return &LengthCounter{path: name, cluster: cluster}, nil
}

func (self *LengthCounter) Increment(allowBlock bool, length int16) error {
	var bucket string
	if allowBlock {
		bucket = self.path + "-allow"
	} else {
		bucket = self.path + "-block"
	}

	key := strconv.Itoa(int(length))

	fmt.Println("Incrementing", bucket, key)

	builder := riak.NewUpdateCounterCommandBuilder()
	cmd, err := builder.WithBucketType("lengths2").
		WithBucket(bucket).
		WithKey(key).
		WithIncrement(1).
		Build()

	if err != nil {
		return err
	}

	return self.cluster.Execute(cmd)
}

func (self *LengthCounter) ProcessBytes(allowBlock bool, sequence []byte) {
	if !self.first {
		self.first = true
	} else {
		return
	}

	length := int16(len(sequence))

	self.Increment(allowBlock, length)

	//	self.bytemap.Save()
}
