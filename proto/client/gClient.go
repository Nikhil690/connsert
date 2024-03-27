// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	context "context"
	// "fmt"
	"math/rand"
	"os"
	"time"

	"github.com/Nikhil690/connsert/logger"
	protos "github.com/Nikhil690/connsert/proto/sdcoreConfig"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

var selfRestartCounter uint32
var configPodRestartCounter uint32 = 0

func init() {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	selfRestartCounter = r1.Uint32()
}

type PlmnId struct {
	MCC string
	MNC string
}

type Nssai struct {
	sst string
	sd  string
}

type ConfigClient struct {
	Client            protos.ConfigServiceClient
	Conn              *grpc.ClientConn
	Channel           chan *protos.NetworkSliceResponse
	Host              string
	Version           string
	MetadataRequested bool
}

type ConfClient interface {
	// channel is created on which subscription is done.
	// On Receiving Configuration from ConfigServer, this api publishes
	// on created channel and returns the channel
	PublishOnConfigChange(bool) chan *protos.NetworkSliceResponse

	//returns grpc connection object
	GetConfigClientConn() *grpc.ClientConn

	//Client Subscribing channel to ConfigPod to receive configuration
	subscribeToConfigPod(commChan chan *protos.NetworkSliceResponse)
}

// This API is added to control metadata from NF Clients
func ConnectToConfigServer(host string) ConfClient {
	confClient := CreateChannel(host, 10000)
	if confClient == nil {
		logger.GrpcLog.Errorln("create grpc channel to config pod failed")
		return nil
	}
	return confClient
}

func (confClient *ConfigClient) PublishOnConfigChange(mdataFlag bool) chan *protos.NetworkSliceResponse {
	confClient.MetadataRequested = mdataFlag
	commChan := make(chan *protos.NetworkSliceResponse)
	confClient.Channel = commChan
	go confClient.subscribeToConfigPod(commChan)
	return commChan
}

// pass structr which has configChangeUpdate interface
func ConfigWatcher() chan *protos.NetworkSliceResponse {
	//var confClient *gClient.ConfigClient
	//TODO: use port from configmap.
	confClient := CreateChannel("webui:9876", 10000)
	if confClient == nil {
		logger.GrpcLog.Errorf("create grpc channel to config pod failed")
		return nil
	}
	commChan := make(chan *protos.NetworkSliceResponse)
	go confClient.subscribeToConfigPod(commChan)
	return commChan
}

func CreateChannel(host string, timeout uint32) ConfClient {
	logger.GrpcLog.Infoln("Config Client : Creating")
	// Second, check to see if we can reuse the gRPC connection for a new P4RT client
	conn, err := newClientConnection(host)
	if err != nil {
		logger.GrpcLog.Errorf("grpc connection failed %v", err)
		return nil
	}

	client := &ConfigClient{
		Client: protos.NewConfigServiceClient(conn),
		Conn:   conn,
		Host:   host,
	}

	return client
}

var kacp = keepalive.ClientParameters{
	Time:                20 * time.Second, // send pings every 20 seconds if there is no activity
	Timeout:             2 * time.Second,  // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: true,             // send pings even without active streams
}

var retryPolicy = `{
		"methodConfig": [{
		  "name": [{"service": "grpc.Config"}],
		  "waitForReady": true,
		  "retryPolicy": {
			  "MaxAttempts": 4,
			  "InitialBackoff": ".01s",
			  "MaxBackoff": ".01s",
			  "BackoffMultiplier": 1.0,
			  "RetryableStatusCodes": [ "UNAVAILABLE" ]
		  }}]}`

func newClientConnection(host string) (conn *grpc.ClientConn, err error) {
	/* get connection */
	logger.GrpcLog.Infoln("Dialing GRPC Connection - ", host)

	bd := 1 * time.Second
	mltpr := 1.0
	jitter := 0.2
	MaxDelay := 5 * time.Second
	bc := backoff.Config{BaseDelay: bd, Multiplier: mltpr, Jitter: jitter, MaxDelay: MaxDelay}

	crt := grpc.ConnectParams{Backoff: bc}
	dialOptions := []grpc.DialOption{grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp), grpc.WithDefaultServiceConfig(retryPolicy), grpc.WithConnectParams(crt)}
	for i := 0; i < 7; i++ {
		logger.GrpcLog.Infoln("Connecting to GRPC ...")
		time.Sleep(time.Second * 10)
	}
	// time.Sleep(time.Second * 60)
	conn, err = grpc.Dial(host, dialOptions...)

	if err != nil {
		logger.GrpcLog.Errorln("grpc dial err: ", err)
		return nil, err
	}
	//defer conn.Close()
	return conn, err
}

func (confClient *ConfigClient) GetConfigClientConn() *grpc.ClientConn {
	return confClient.Conn
}

func (confClient *ConfigClient) subscribeToConfigPod(commChan chan *protos.NetworkSliceResponse) {
	logger.GrpcLog.Infoln("Subscribing to Config POD")
	myid := os.Getenv("HOSTNAME")
	var stream protos.ConfigService_NetworkSliceSubscribeClient
	// var rst protos.NetworkSliceResponse
	// Define a label for the outer loop
retry:
	for {
		if stream == nil {
			status := confClient.Conn.GetState()
			var err error
			if status == connectivity.Ready {
				logger.GrpcLog.Infoln("Connectivity status: Ready")
				rreq := &protos.NetworkSliceRequest{RestartCounter: selfRestartCounter, ClientId: myid, MetadataRequested: confClient.MetadataRequested}
				if stream, err = confClient.Client.NetworkSliceSubscribe(context.Background(), rreq); err != nil {
					logger.GrpcLog.Errorf("Failed to subscribe: %v", err)
					time.Sleep(time.Second * 5)
					// Retry on failure
					continue
				}
			} else if status == connectivity.Idle {
				logger.GrpcLog.Errorf("connecting...")
				time.Sleep(time.Second * 5)
				continue
			} else {
				logger.GrpcLog.Errorf("Connectivity status: Not Ready")
				time.Sleep(time.Second * 1)
				// Restart the entire loop
				goto retry
			}
		}
		rsp, err := stream.Recv()
		if err != nil {
			logger.GrpcLog.Errorf("Failed to receive message: %v", err)
			// Clearing the stream will force the client to resubscribe on the next iteration
			stream = nil
			time.Sleep(time.Second * 5)
			// Retry on failure
			continue
		}

		logger.GrpcLog.Infoln("Config Message received ")
		logger.GrpcLog.Debugf("#Network Slices %v, RC of configpod %v ", len(rsp.NetworkSlice), rsp.RestartCounter)
		if configPodRestartCounter == 0 || (configPodRestartCounter == rsp.RestartCounter) {
			// first time connection or config update
			configPodRestartCounter = rsp.RestartCounter
			if len(rsp.NetworkSlice) > 0 {
				// always carries full config copy
				logger.GrpcLog.Infoln("Initial Config Received: ")
				// logger.GrpcLog.Infoln(rsp)
				logger.GrpcLog.Info("+---------------------------------------------+")
				logger.GrpcLog.Infof("| %-43s |\n", "Network Slice")
				logger.GrpcLog.Infof("|---------------------------------------------|")
				// logger.GrpcLog.Infof("| %15s | %10d |\n", "RestartCounter", rsp.RestartCounter)
				// logger.GrpcLog.Infof("| %15s | %10d |\n", "ConfigUpdated", rsp.ConfigUpdated)
				for _, slice := range rsp.NetworkSlice {
					logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Name", slice.Name)
					logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Sst", slice.Nssai.Sst)
					logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Sd", slice.Nssai.Sd)
					logger.GrpcLog.Infof("|---------------------------------------------|")
					for _, group := range slice.DeviceGroup {
						logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Device Group", group.Name)
						logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "IP Domain Details", group.IpDomainDetails.Name)
						logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "DNN Name", group.IpDomainDetails.DnnName)
						logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "UE Pool", group.IpDomainDetails.UePool)
						logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "DNS Primary", group.IpDomainDetails.DnsPrimary)
						logger.GrpcLog.Infof("| %-18s  | %-21d |\n", "MTU", group.IpDomainDetails.Mtu)
						logger.GrpcLog.Infof("| %-18s  | %-21d |\n", "DnnMbrUplink", group.IpDomainDetails.UeDnnQos.DnnMbrUplink)
						logger.GrpcLog.Infof("| %-18s  | %-21d |\n", "DnnMbrDownlink", group.IpDomainDetails.UeDnnQos.DnnMbrDownlink)
						logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Traffic Class", group.IpDomainDetails.UeDnnQos.TrafficClass.Name)
						// logger.GrpcLog.Infof("| %-18s  | %-21d |\n", "QCI", group.IpDomainDetails.UeDnnQos.TrafficClass.Qci)
						// logger.GrpcLog.Infof("| %-18s  | %-21d |\n", "ARP", group.IpDomainDetails.UeDnnQos.TrafficClass.Arp)
						// logger.GrpcLog.Infof("| %-18s  | %-21d |\n", "PDB", group.IpDomainDetails.UeDnnQos.TrafficClass.Pdb)
						// logger.GrpcLog.Infof("| %-18s  | %-21d |\n", "PELR", group.IpDomainDetails.UeDnnQos.TrafficClass.Pelr)
						for i, imdetails := range group.Imsi {
							label := ""
							if i == len(group.Imsi)/2 {
								label = "IMSI_LIST"
							}
							logger.GrpcLog.Infof("| %-18s  | %-21s |\n", label, imdetails)
						}
						logger.GrpcLog.Info("|---------------------------------------------|")
					}
					logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Site", slice.Site.SiteName)
					for _, gnb := range slice.Site.Gnb {
						logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "GNB", gnb.Name)
						logger.GrpcLog.Infof("| %-18s  | %-21d |\n", "TAC", gnb.Tac)
						logger.GrpcLog.Info("|---------------------------------------------|")
					}
					logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "MCC", slice.Site.Plmn.Mcc)
					logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "MNC", slice.Site.Plmn.Mnc)
					logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "UPF", slice.Site.Upf.UpfName)
					for _, appfilter := range slice.AppFilters.PccRuleBase {
						for _, flowinfo := range appfilter.FlowInfos {
							// logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Flow Description", flowinfo.FlowDesc)
							logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Traffic Class", flowinfo.TosTrafficClass)
							logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Flow Direction", flowinfo.FlowDir)
							logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Flow Status", flowinfo.FlowStatus)
						}
						logger.GrpcLog.Infof("| %-18s  | %-21s |\n", "Rule ID", appfilter.RuleId)
						logger.GrpcLog.Infof("| %-18s  | %-21d |\n", "Priority", appfilter.Priority)
					}
					logger.GrpcLog.Info("|---------------------------------------------|")
				}
				logger.GrpcLog.Info("+---------------------------------------------+")
				commChan <- rsp
			} else if rsp.ConfigUpdated == 1 {
				// config delete , all slices deleted
				logger.GrpcLog.Infoln("Complete config deleted ")
				commChan <- rsp
			}
		} else if len(rsp.NetworkSlice) > 0 {
			logger.GrpcLog.Errorf("Config received after config Pod restart")
			//config received after config pod restart
			configPodRestartCounter = rsp.RestartCounter
			commChan <- rsp
		} else {
			logger.GrpcLog.Errorf("Config Pod is restarted and no config received")
		}
	}
}
func readConfigInLoop(confClient *ConfigClient, commChan chan *protos.NetworkSliceResponse) {
	myid := os.Getenv("HOSTNAME")
	configReadTimeout := time.NewTicker(5000 * time.Millisecond)
	for {
		select {
		case <-configReadTimeout.C:
			status := confClient.Conn.GetState()
			if status == connectivity.Ready {
				rreq := &protos.NetworkSliceRequest{RestartCounter: selfRestartCounter, ClientId: myid, MetadataRequested: confClient.MetadataRequested}
				rsp, err := confClient.Client.GetNetworkSlice(context.Background(), rreq)
				if err != nil {
					logger.GrpcLog.Errorln("read Network Slice config from webconsole failed : ", err)
					continue
				}
				logger.GrpcLog.Debugf("#Network Slices %v, RC of configpod %v ", len(rsp.NetworkSlice), rsp.RestartCounter)
				if configPodRestartCounter == 0 || (configPodRestartCounter == rsp.RestartCounter) {
					// first time connection or config update
					configPodRestartCounter = rsp.RestartCounter
					if len(rsp.NetworkSlice) > 0 {
						// always carries full config copy
						logger.GrpcLog.Infoln("First time config Received ", rsp)
						commChan <- rsp
					} else if rsp.ConfigUpdated == 1 {
						// config delete , all slices deleted
						logger.GrpcLog.Infoln("Complete config deleted ")
						commChan <- rsp
					}
				} else if len(rsp.NetworkSlice) > 0 {
					logger.GrpcLog.Errorf("Config received after config Pod restart")
					//config received after config pod restart
					configPodRestartCounter = rsp.RestartCounter
					commChan <- rsp
				} else {
					logger.GrpcLog.Errorf("Config Pod is restarted and no config received")
				}
			} else {
				logger.GrpcLog.Errorln("read Network Slice config from webconsole skipped. GRPC channel down ")
			}
		}
	}
}
