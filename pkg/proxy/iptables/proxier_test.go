/*
Copyright 2015 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package iptables

import (
	"testing"

	"fmt"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/proxy"
	"k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/util/exec"
	utiliptables "k8s.io/kubernetes/pkg/util/iptables"
	"net"
	"strings"
)

func checkAllLines(t *testing.T, table utiliptables.Table, save []byte, expectedLines map[utiliptables.Chain]string) {
	chainLines := getChainLines(table, save)
	for chain, line := range chainLines {
		if expected, exists := expectedLines[chain]; exists {
			if expected != line {
				t.Errorf("getChainLines expected chain line not present. For chain: %s Expected: %s Got: %s", chain, expected, line)
			}
		} else {
			t.Errorf("getChainLines expected chain not present: %s", chain)
		}
	}
}

func TestReadLinesFromByteBuffer(t *testing.T) {
	testFn := func(byteArray []byte, expected []string) {
		index := 0
		readIndex := 0
		for ; readIndex < len(byteArray); index++ {
			line, n := readLine(readIndex, byteArray)
			readIndex = n
			if expected[index] != line {
				t.Errorf("expected:%q, actual:%q", expected[index], line)
			}
		} // for
		if readIndex < len(byteArray) {
			t.Errorf("Byte buffer was only partially read. Buffer length is:%d, readIndex is:%d", len(byteArray), readIndex)
		}
		if index < len(expected) {
			t.Errorf("All expected strings were not compared. expected arr length:%d, matched count:%d", len(expected), index-1)
		}
	}

	byteArray1 := []byte("\n  Line 1  \n\n\n L ine4  \nLine 5 \n \n")
	expected1 := []string{"", "Line 1", "", "", "L ine4", "Line 5", ""}
	testFn(byteArray1, expected1)

	byteArray1 = []byte("")
	expected1 = []string{}
	testFn(byteArray1, expected1)

	byteArray1 = []byte("\n\n")
	expected1 = []string{"", ""}
	testFn(byteArray1, expected1)
}

func TestGetChainLines(t *testing.T) {
	iptables_save := `# Generated by iptables-save v1.4.7 on Wed Oct 29 14:56:01 2014
	*nat
	:PREROUTING ACCEPT [2136997:197881818]
	:POSTROUTING ACCEPT [4284525:258542680]
	:OUTPUT ACCEPT [5901660:357267963]
	-A PREROUTING -m addrtype --dst-type LOCAL -j DOCKER
	COMMIT
	# Completed on Wed Oct 29 14:56:01 2014`
	expected := map[utiliptables.Chain]string{
		utiliptables.ChainPrerouting:  ":PREROUTING ACCEPT [2136997:197881818]",
		utiliptables.ChainPostrouting: ":POSTROUTING ACCEPT [4284525:258542680]",
		utiliptables.ChainOutput:      ":OUTPUT ACCEPT [5901660:357267963]",
	}
	checkAllLines(t, utiliptables.TableNAT, []byte(iptables_save), expected)
}

func TestGetChainLinesMultipleTables(t *testing.T) {
	iptables_save := `# Generated by iptables-save v1.4.21 on Fri Aug  7 14:47:37 2015
	*nat
	:PREROUTING ACCEPT [2:138]
	:INPUT ACCEPT [0:0]
	:OUTPUT ACCEPT [0:0]
	:POSTROUTING ACCEPT [0:0]
	:DOCKER - [0:0]
	:KUBE-NODEPORT-CONTAINER - [0:0]
	:KUBE-NODEPORT-HOST - [0:0]
	:KUBE-PORTALS-CONTAINER - [0:0]
	:KUBE-PORTALS-HOST - [0:0]
	:KUBE-SVC-1111111111111111 - [0:0]
	:KUBE-SVC-2222222222222222 - [0:0]
	:KUBE-SVC-3333333333333333 - [0:0]
	:KUBE-SVC-4444444444444444 - [0:0]
	:KUBE-SVC-5555555555555555 - [0:0]
	:KUBE-SVC-6666666666666666 - [0:0]
	-A PREROUTING -m comment --comment "handle ClusterIPs; NOTE: this must be before the NodePort rules" -j KUBE-PORTALS-CONTAINER
	-A PREROUTING -m addrtype --dst-type LOCAL -j DOCKER
	-A PREROUTING -m addrtype --dst-type LOCAL -m comment --comment "handle service NodePorts; NOTE: this must be the last rule in the chain" -j KUBE-NODEPORT-CONTAINER
	-A OUTPUT -m comment --comment "handle ClusterIPs; NOTE: this must be before the NodePort rules" -j KUBE-PORTALS-HOST
	-A OUTPUT ! -d 127.0.0.0/8 -m addrtype --dst-type LOCAL -j DOCKER
	-A OUTPUT -m addrtype --dst-type LOCAL -m comment --comment "handle service NodePorts; NOTE: this must be the last rule in the chain" -j KUBE-NODEPORT-HOST
	-A POSTROUTING -s 10.246.1.0/24 ! -o cbr0 -j MASQUERADE
	-A POSTROUTING -s 10.0.2.15/32 -d 10.0.2.15/32 -m comment --comment "handle pod connecting to self" -j MASQUERADE
	-A KUBE-PORTALS-CONTAINER -d 10.247.0.1/32 -p tcp -m comment --comment "portal for default/kubernetes:" -m state --state NEW -m tcp --dport 443 -j KUBE-SVC-5555555555555555
	-A KUBE-PORTALS-CONTAINER -d 10.247.0.10/32 -p udp -m comment --comment "portal for kube-system/kube-dns:dns" -m state --state NEW -m udp --dport 53 -j KUBE-SVC-6666666666666666
	-A KUBE-PORTALS-CONTAINER -d 10.247.0.10/32 -p tcp -m comment --comment "portal for kube-system/kube-dns:dns-tcp" -m state --state NEW -m tcp --dport 53 -j KUBE-SVC-2222222222222222
	-A KUBE-PORTALS-HOST -d 10.247.0.1/32 -p tcp -m comment --comment "portal for default/kubernetes:" -m state --state NEW -m tcp --dport 443 -j KUBE-SVC-5555555555555555
	-A KUBE-PORTALS-HOST -d 10.247.0.10/32 -p udp -m comment --comment "portal for kube-system/kube-dns:dns" -m state --state NEW -m udp --dport 53 -j KUBE-SVC-6666666666666666
	-A KUBE-PORTALS-HOST -d 10.247.0.10/32 -p tcp -m comment --comment "portal for kube-system/kube-dns:dns-tcp" -m state --state NEW -m tcp --dport 53 -j KUBE-SVC-2222222222222222
	-A KUBE-SVC-1111111111111111 -p udp -m comment --comment "kube-system/kube-dns:dns" -m recent --set --name KUBE-SVC-1111111111111111 --mask 255.255.255.255 --rsource -j DNAT --to-destination 10.246.1.2:53
	-A KUBE-SVC-2222222222222222 -m comment --comment "kube-system/kube-dns:dns-tcp" -j KUBE-SVC-3333333333333333
	-A KUBE-SVC-3333333333333333 -p tcp -m comment --comment "kube-system/kube-dns:dns-tcp" -m recent --set --name KUBE-SVC-3333333333333333 --mask 255.255.255.255 --rsource -j DNAT --to-destination 10.246.1.2:53
	-A KUBE-SVC-4444444444444444 -p tcp -m comment --comment "default/kubernetes:" -m recent --set --name KUBE-SVC-4444444444444444 --mask 255.255.255.255 --rsource -j DNAT --to-destination 10.245.1.2:443
	-A KUBE-SVC-5555555555555555 -m comment --comment "default/kubernetes:" -j KUBE-SVC-4444444444444444
	-A KUBE-SVC-6666666666666666 -m comment --comment "kube-system/kube-dns:dns" -j KUBE-SVC-1111111111111111
	COMMIT
	# Completed on Fri Aug  7 14:47:37 2015
	# Generated by iptables-save v1.4.21 on Fri Aug  7 14:47:37 2015
	*filter
	:INPUT ACCEPT [17514:83115836]
	:FORWARD ACCEPT [0:0]
	:OUTPUT ACCEPT [8909:688225]
	:DOCKER - [0:0]
	-A FORWARD -o cbr0 -j DOCKER
	-A FORWARD -o cbr0 -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
	-A FORWARD -i cbr0 ! -o cbr0 -j ACCEPT
	-A FORWARD -i cbr0 -o cbr0 -j ACCEPT
	COMMIT
	`
	expected := map[utiliptables.Chain]string{
		utiliptables.ChainPrerouting:                    ":PREROUTING ACCEPT [2:138]",
		utiliptables.Chain("INPUT"):                     ":INPUT ACCEPT [0:0]",
		utiliptables.Chain("OUTPUT"):                    ":OUTPUT ACCEPT [0:0]",
		utiliptables.ChainPostrouting:                   ":POSTROUTING ACCEPT [0:0]",
		utiliptables.Chain("DOCKER"):                    ":DOCKER - [0:0]",
		utiliptables.Chain("KUBE-NODEPORT-CONTAINER"):   ":KUBE-NODEPORT-CONTAINER - [0:0]",
		utiliptables.Chain("KUBE-NODEPORT-HOST"):        ":KUBE-NODEPORT-HOST - [0:0]",
		utiliptables.Chain("KUBE-PORTALS-CONTAINER"):    ":KUBE-PORTALS-CONTAINER - [0:0]",
		utiliptables.Chain("KUBE-PORTALS-HOST"):         ":KUBE-PORTALS-HOST - [0:0]",
		utiliptables.Chain("KUBE-SVC-1111111111111111"): ":KUBE-SVC-1111111111111111 - [0:0]",
		utiliptables.Chain("KUBE-SVC-2222222222222222"): ":KUBE-SVC-2222222222222222 - [0:0]",
		utiliptables.Chain("KUBE-SVC-3333333333333333"): ":KUBE-SVC-3333333333333333 - [0:0]",
		utiliptables.Chain("KUBE-SVC-4444444444444444"): ":KUBE-SVC-4444444444444444 - [0:0]",
		utiliptables.Chain("KUBE-SVC-5555555555555555"): ":KUBE-SVC-5555555555555555 - [0:0]",
		utiliptables.Chain("KUBE-SVC-6666666666666666"): ":KUBE-SVC-6666666666666666 - [0:0]",
	}
	checkAllLines(t, utiliptables.TableNAT, []byte(iptables_save), expected)
}

func TestGetRemovedEndpoints(t *testing.T) {
	testCases := []struct {
		currentEndpoints []string
		newEndpoints     []string
		removedEndpoints []string
	}{
		{
			currentEndpoints: []string{"10.0.2.1:80", "10.0.2.2:80"},
			newEndpoints:     []string{"10.0.2.1:80", "10.0.2.2:80"},
			removedEndpoints: []string{},
		},
		{
			currentEndpoints: []string{"10.0.2.1:80", "10.0.2.2:80", "10.0.2.3:80"},
			newEndpoints:     []string{"10.0.2.1:80", "10.0.2.2:80"},
			removedEndpoints: []string{"10.0.2.3:80"},
		},
		{
			currentEndpoints: []string{},
			newEndpoints:     []string{"10.0.2.1:80", "10.0.2.2:80"},
			removedEndpoints: []string{},
		},
		{
			currentEndpoints: []string{"10.0.2.1:80", "10.0.2.2:80"},
			newEndpoints:     []string{},
			removedEndpoints: []string{"10.0.2.1:80", "10.0.2.2:80"},
		},
		{
			currentEndpoints: []string{"10.0.2.1:80", "10.0.2.2:80", "10.0.2.2:443"},
			newEndpoints:     []string{"10.0.2.1:80", "10.0.2.2:80"},
			removedEndpoints: []string{"10.0.2.2:443"},
		},
	}

	for i := range testCases {
		res := getRemovedEndpoints(testCases[i].currentEndpoints, testCases[i].newEndpoints)
		if !slicesEquiv(res, testCases[i].removedEndpoints) {
			t.Errorf("Expected: %v, but getRemovedEndpoints returned: %v", testCases[i].removedEndpoints, res)
		}
	}
}

func TestExecConntrackTool(t *testing.T) {
	fcmd := exec.FakeCmd{
		CombinedOutputScript: []exec.FakeCombinedOutputAction{
			func() ([]byte, error) { return []byte("1 flow entries have been deleted"), nil },
			func() ([]byte, error) { return []byte("1 flow entries have been deleted"), nil },
			func() ([]byte, error) {
				return []byte(""), fmt.Errorf("conntrack v1.4.2 (conntrack-tools): 0 flow entries have been deleted.")
			},
		},
	}
	fexec := exec.FakeExec{
		CommandScript: []exec.FakeCommandAction{
			func(cmd string, args ...string) exec.Cmd { return exec.InitFakeCmd(&fcmd, cmd, args...) },
			func(cmd string, args ...string) exec.Cmd { return exec.InitFakeCmd(&fcmd, cmd, args...) },
			func(cmd string, args ...string) exec.Cmd { return exec.InitFakeCmd(&fcmd, cmd, args...) },
		},
		LookPathFunc: func(cmd string) (string, error) { return cmd, nil },
	}

	fakeProxier := Proxier{exec: &fexec}

	testCases := [][]string{
		{"-L", "-p", "udp"},
		{"-D", "-p", "udp", "-d", "10.0.240.1"},
		{"-D", "-p", "udp", "--orig-dst", "10.240.0.2", "--dst-nat", "10.0.10.2"},
	}

	expectErr := []bool{false, false, true}

	for i := range testCases {
		err := fakeProxier.execConntrackTool(testCases[i]...)

		if expectErr[i] {
			if err == nil {
				t.Errorf("expected err, got %v", err)
			}
		} else {
			if err != nil {
				t.Errorf("expected success, got %v", err)
			}
		}

		execCmd := strings.Join(fcmd.CombinedOutputLog[i], " ")
		expectCmd := fmt.Sprintf("%s %s", "conntrack", strings.Join(testCases[i], " "))

		if execCmd != expectCmd {
			t.Errorf("expect execute command: %s, but got: %s", expectCmd, execCmd)
		}
	}
}

func newFakeServiceInfo(service proxy.ServicePortName, ip net.IP, protocol api.Protocol) *serviceInfo {
	return &serviceInfo{
		sessionAffinityType: api.ServiceAffinityNone, // default
		stickyMaxAgeSeconds: 180,                     // TODO: paramaterize this in the API.
		clusterIP:           ip,
		protocol:            protocol,
	}
}

func TestDeleteEndpointConnections(t *testing.T) {
	fcmd := exec.FakeCmd{
		CombinedOutputScript: []exec.FakeCombinedOutputAction{
			func() ([]byte, error) { return []byte("1 flow entries have been deleted"), nil },
			func() ([]byte, error) {
				return []byte(""), fmt.Errorf("conntrack v1.4.2 (conntrack-tools): 0 flow entries have been deleted.")
			},
		},
	}
	fexec := exec.FakeExec{
		CommandScript: []exec.FakeCommandAction{
			func(cmd string, args ...string) exec.Cmd { return exec.InitFakeCmd(&fcmd, cmd, args...) },
			func(cmd string, args ...string) exec.Cmd { return exec.InitFakeCmd(&fcmd, cmd, args...) },
		},
		LookPathFunc: func(cmd string) (string, error) { return cmd, nil },
	}

	serviceMap := make(map[proxy.ServicePortName]*serviceInfo)
	svc1 := proxy.ServicePortName{types.NamespacedName{Namespace: "ns1", Name: "svc1"}, ""}
	svc2 := proxy.ServicePortName{types.NamespacedName{Namespace: "ns1", Name: "svc2"}, ""}
	serviceMap[svc1] = newFakeServiceInfo(svc1, net.IPv4(10, 20, 30, 40), api.ProtocolUDP)
	serviceMap[svc2] = newFakeServiceInfo(svc1, net.IPv4(10, 20, 30, 41), api.ProtocolTCP)

	fakeProxier := Proxier{exec: &fexec, serviceMap: serviceMap}

	testCases := []endpointServicePair{
		{
			endpoint:        "10.240.0.3:80",
			servicePortName: svc1,
		},
		{
			endpoint:        "10.240.0.4:80",
			servicePortName: svc1,
		},
		{
			endpoint:        "10.240.0.5:80",
			servicePortName: svc2,
		},
	}

	expectCommandExecCount := 0
	for i := range testCases {
		input := map[endpointServicePair]bool{testCases[i]: true}
		fakeProxier.deleteEndpointConnections(input)
		svcInfo := fakeProxier.serviceMap[testCases[i].servicePortName]
		if svcInfo.protocol == api.ProtocolUDP {
			svcIp := svcInfo.clusterIP.String()
			endpointIp := strings.Split(testCases[i].endpoint, ":")[0]
			expectCommand := fmt.Sprintf("conntrack -D --orig-dst %s --dst-nat %s -p udp", svcIp, endpointIp)
			execCommand := strings.Join(fcmd.CombinedOutputLog[expectCommandExecCount], " ")
			if expectCommand != execCommand {
				t.Errorf("Exepect comand: %s, but executed %s", expectCommand, execCommand)
			}
			expectCommandExecCount += 1
		}

		if expectCommandExecCount != fexec.CommandCalls {
			t.Errorf("Exepect comand executed %d times, but got %d", expectCommandExecCount, fexec.CommandCalls)
		}
	}
}

func TestDeleteServiceConnections(t *testing.T) {
	fcmd := exec.FakeCmd{
		CombinedOutputScript: []exec.FakeCombinedOutputAction{
			func() ([]byte, error) { return []byte("1 flow entries have been deleted"), nil },
			func() ([]byte, error) { return []byte("1 flow entries have been deleted"), nil },
			func() ([]byte, error) {
				return []byte(""), fmt.Errorf("conntrack v1.4.2 (conntrack-tools): 0 flow entries have been deleted.")
			},
		},
	}
	fexec := exec.FakeExec{
		CommandScript: []exec.FakeCommandAction{
			func(cmd string, args ...string) exec.Cmd { return exec.InitFakeCmd(&fcmd, cmd, args...) },
			func(cmd string, args ...string) exec.Cmd { return exec.InitFakeCmd(&fcmd, cmd, args...) },
			func(cmd string, args ...string) exec.Cmd { return exec.InitFakeCmd(&fcmd, cmd, args...) },
		},
		LookPathFunc: func(cmd string) (string, error) { return cmd, nil },
	}

	fakeProxier := Proxier{exec: &fexec}

	testCases := [][]string{
		{
			"10.240.0.3",
			"10.240.0.5",
		},
		{
			"10.240.0.4",
		},
	}

	svcCount := 0
	for i := range testCases {
		fakeProxier.deleteServiceConnections(testCases[i])
		for _, ip := range testCases[i] {
			expectCommand := fmt.Sprintf("conntrack -D --orig-dst %s -p udp", ip)
			execCommand := strings.Join(fcmd.CombinedOutputLog[svcCount], " ")
			if expectCommand != execCommand {
				t.Errorf("Exepect comand: %s, but executed %s", expectCommand, execCommand)
			}
			svcCount += 1
		}
		if svcCount != fexec.CommandCalls {
			t.Errorf("Exepect comand executed %d times, but got %d", svcCount, fexec.CommandCalls)
		}
	}
}

// TODO(thockin): add a test for syncProxyRules() or break it down further and test the pieces.
