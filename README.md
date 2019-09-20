# The Operator Foundation

[Operator](https://operatorfoundation.org) makes useable tools to help people around the world with censorship, security, and privacy.

## Adversary Lab

Adversary Lab is a service that analyzes captured network traffic to extract statistical properties. Using this analysis, filtering rules can be synthesized to block sampled traffic.

The purpose of Adversary Lab is to give researchers and developers studying network filtering a way to understand how easy it is to block different protocols.
If you have an application that uses a custom protocol, Adversary Lab will demonstrate how a rule can be synthesized to systematically block all traffic using that protocol.
Similarly, if you have a network filtering circumvention tool, then Adversary Lab can synthesize a rule to block your tool.
This analysis can also be used to study tools that specifically attempt to defeat networking filtering, such as Pluggable Transports.

Adversary Lab analysis works by training a classifier on two observed data sets, the "allow" set and the "block" set.
For instance, a simulated adversary could allow HTTP, but block HTTPS. By training the system with HTTP and HTTPS data, it will generate a rule that distinguishes these two classes of traffic based on properties observed in the traffic.

## AdversaryLabClient

AdversaryLabClient is a command line tool which captures traffic and submits it to AdversaryLab for analysis.

#### Installation

Adversary Lab is written in the Go programming language. To compile it you need
to install Go 1.7 or higher:

<https://golang.org/doc/install>

If you just installed Go for the first time, you will need to create a directory
to keep all of your Go source code:

    mkdir ~/go

If you already have Go installed, make sure it is a compatible version:

    go version

The version should be 1.7 or higher.

If you get the error "go: command not found", then trying exiting your terminal
and starting a new one.

If you have a compatible Go installed, you should go to the directory where you
keep all of your Go source code and set your GOPATH:

    cd ~/go
    export GOPATH=~/go

Software written in Go is installed using the `go get` command:

    go get -u github.com/OperatorFoundation/AdversaryLabClient

This will fetch the source code for the Adversary Lab command line client, and
all the dependencies, compile everything, and put the result in
bin/AdversaryLabClient.

#### Running

To use the client, Adversary Lab must already be running. See the [AdversaryLab documentation](https://github.com/OperatorFoundation/AdversaryLab) to set up and run AdversaryLab.

To interface with the AdversaryLab service, you need to use the command client.

Run the command line client without argument to get usage information:

    bin/AdversaryLabClient

**Train a simulated adversary by capturing network traffic that is designated as either allowed or blocked:**

    sudo bin/AdversaryLabClient 80 allow

This will capture live traffic with a destination port of 80 and add it to the dataset as training for what traffic the adversary should allow.

We will also need to train the simulated adversary using captured network traffic that gives an example of what to block:

    sudo bin/AdversaryLabClient 443 block

This will capture live traffic with a destination port of 443 and add it to the "example" dataset as training for what traffic the adversary should block.

**Alternately you can capture network traffic first and assign the data as either allowed or blocked when you have are done capturing traffic:**

In this scenario you simply leave off the allow/block designation and only provide the port you would like Adversary Lab to listen on. Adversary lab will buffer your traffic until you type in either "allow" or "block". At which point it will stop recording and add it to the dataset based on your input.

For example:

    sudo bin/AdversaryLabClient 443

or

    sudo bin/AdversaryLabClient 80

Once the simulated adversary has both "allow" and "block" traffic, and has observed at least three connections from each type, it can synthesize blocking rules.
