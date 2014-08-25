/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.storm.perftest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import backtype.storm.utils.NimbusClient;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.ExecutorStats;

public class Main {
  private static final Log LOG = LogFactory.getLog(Main.class);
  
	private final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
	private final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "bolt.parallel";
	private final static String TOPOLOGY_NUMS="topology.nums";
  //number of levels of bolts per topolgy
	private final static String TOPOLOGY_BOLTS_NUMS="topology.bolt.nums";
	
	//How often should metrics be collected
	private final static String TOPOLOGY_POLLFREQSEC="topology.pollFreq";
	//How long should the benchmark run for
	private final static String TOPOLOGY_RUNTIMESEC="topology.testRunTime";
	//message size
	private final static String TOPOLOGY_MESSAGE_SIZES="topology.messagesize";

  private static class MetricsState {
    long transferred = 0;
    int slotsUsed = 0;
    long lastTime = 0;
  }

	private static Map conf = new HashMap<Object, Object>();

	private static void LoadProperty(String prop) {
		Properties properties = new Properties();

		try {
			InputStream stream = new FileInputStream(prop);
			properties.load(stream);
		} catch (FileNotFoundException e) {
			System.out.println("No such file " + prop);
		} catch (Exception e1) {
			e1.printStackTrace();

			return;
		}

		conf.putAll(properties);
	}

  public void metrics(Nimbus.Client client, int size, int poll, int total) throws Exception {
    System.out.println("status\ttopologies\ttotalSlots\tslotsUsed\ttotalExecutors\texecutorsWithMetrics\ttime\ttime-diff ms\ttransferred\tthroughput (MB/s)");
    MetricsState state = new MetricsState();
    long pollMs = poll * 1000;
    long now = System.currentTimeMillis();
    state.lastTime = now;
    long startTime = now;
    long cycle = 0;
    long sleepTime;
    long wakeupTime;
    int nPolls=0;
    while (metrics(client, size, now, state, "WAITING")) {
      now = System.currentTimeMillis();
      cycle = (now - startTime)/pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    }
    long wantingtransferred=state.transferred;
    

    now = System.currentTimeMillis();
    cycle = (now - startTime)/pollMs;
    wakeupTime = startTime + (pollMs * (cycle + 1));
    sleepTime = wakeupTime - now;
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
    now = System.currentTimeMillis();
    long end = now + (total * 1000);
    
    do {
      metrics(client, size, now, state, "RUNNING");
      now = System.currentTimeMillis();
      nPolls++;
      cycle = (now - startTime)/pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    } while (now < end);
    long avgtransferred=(state.transferred-wantingtransferred)/nPolls;
    System.out.println("RUNNING " + nPolls + " Polls, AVG_Transferred = " + avgtransferred);

  }

  public boolean metrics(Nimbus.Client client, int size, long now, MetricsState state, String message) throws Exception {
    ClusterSummary summary = client.getClusterInfo();
    long time = now - state.lastTime;
    state.lastTime = now;
    int numSupervisors = summary.get_supervisors_size();
    int totalSlots = 0;
    int totalUsedSlots = 0;
    for (SupervisorSummary sup: summary.get_supervisors()) {
      totalSlots += sup.get_num_workers();
      totalUsedSlots += sup.get_num_used_workers();
    }
    int slotsUsedDiff = totalUsedSlots - state.slotsUsed;
    state.slotsUsed = totalUsedSlots;

    int numTopologies = summary.get_topologies_size();
    long totalTransferred = 0;
    int totalExecutors = 0;
    int executorsWithMetrics = 0;
    for (TopologySummary ts: summary.get_topologies()) {
      String id = ts.get_id();
      TopologyInfo info = client.getTopologyInfo(id);
      for (ExecutorSummary es: info.get_executors()) {
    	//  System.out.println(es.get_component_id());
    	  
    	 if( "messageSpout".equals(es.get_component_id()))
    	 {
	        ExecutorStats stats = es.get_stats();
	        totalExecutors++;
	        if (stats != null) {
	          Map<String,Map<String,Long>> transferred = stats.get_transferred();
	          if ( transferred != null) {
	            Map<String, Long> e2 = transferred.get(":all-time");
	            if (e2 != null) {
	              executorsWithMetrics++;
	              //The SOL messages are always on the default stream, so just count those
	              Long dflt = e2.get("default");
	         //     System.out.println(dflt);
	              if (dflt != null) {
	                totalTransferred += dflt;
	              }
	            }
	          }
	        }
    	 }
      }
    }
    long transferredDiff = totalTransferred - state.transferred;
    state.transferred = totalTransferred;
    double throughput = (transferredDiff == 0 || time == 0) ? 0.0 : (transferredDiff * size)/(1024.0 * 1024.0)/(time/1000.0);
    System.out.println(message+"\t"+numTopologies+"\t"+totalSlots+"\t"+totalUsedSlots+"\t"+totalExecutors+"\t"+executorsWithMetrics+"\t"+now+"\t"+time+"\t"+transferredDiff+"\t"+throughput);
    if ("WAITING".equals(message)) {
      //System.err.println(" !("+totalUsedSlots+" > 0 && "+slotsUsedDiff+" == 0 && "+totalExecutors+" > 0 && "+executorsWithMetrics+" >= "+totalExecutors+")");
    }
    return !(totalUsedSlots > 0 && slotsUsedDiff == 0 && totalExecutors > 0 && executorsWithMetrics >= totalExecutors);
  } 
	public static Integer parseInt(Object o) {
		if (o == null) {
			return -1;
		}
		if (o instanceof String) {
			return Integer.parseInt(String.valueOf(o));
		} else if (o instanceof Long) {
			long value = (Long) o;
			return Integer.valueOf((int) value);
		} else if (o instanceof Integer) {
			return (Integer) o;
		} else 
		  return -1;
	}
  public void realMain(String[] args) throws Exception {
	LoadProperty(args[0]);
    Map clusterConf = Utils.readStormConfig();
    clusterConf.putAll(Utils.readCommandLineOpts());
    Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

    int _numWorkers=parseInt(conf.get(Config.TOPOLOGY_WORKERS));
    
    if (_numWorkers <= 0) {
      throw new IllegalArgumentException("Need at least one worker");
    }
    boolean _ackEnabled=false;
    int _ackers=parseInt(conf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
    if (_ackers>0) {
    	_ackEnabled =true;
    }
    	
        int _numTopologies=parseInt(conf.get(TOPOLOGY_NUMS));
 		int _spoutParallel = parseInt(
 				conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT));
 		int _boltParallel = parseInt(
 				conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT));
 		String _name = (String) conf.get(Config.TOPOLOGY_NAME);
 		if (_name == null) 
 			_name = "MetricTest_storm";
 		int _messageSize=parseInt(conf.get(TOPOLOGY_MESSAGE_SIZES));
 		

    try {
      for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
        TopologyBuilder builder = new TopologyBuilder();
        LOG.info("Adding in "+_spoutParallel+" spouts");
        builder.setSpout("messageSpout", 
            new SOLSpout(_messageSize, _ackEnabled), _spoutParallel);
        LOG.info("Adding in "+_boltParallel+" bolts");
        builder.setBolt("messageBolt1", new SOLBolt(), _boltParallel)
            .shuffleGrouping("messageSpout");
        for (int levelNum = 2; levelNum <= parseInt(conf.get(TOPOLOGY_BOLTS_NUMS)); levelNum++) {
          LOG.info("Adding in "+_boltParallel+" bolts at level "+levelNum);
          builder.setBolt("messageBolt"+levelNum, new SOLBolt(), _boltParallel)
              .shuffleGrouping("messageBolt"+(levelNum - 1));
        }
        int pending=parseInt(conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
        if(pending>1)
        	  Config.setMaxSpoutPending(conf,pending);
        	
  
        Config.setDebug(conf,false);
        Config.setNumWorkers(conf,_numWorkers);
        Config.setNumAckers(conf,_ackers);
        
      
        StormSubmitter.submitTopology(_name+"_"+topoNum, conf, builder.createTopology());
      }
      int  _pollFreqSec=parseInt(conf.get(TOPOLOGY_POLLFREQSEC));
      int  _testRunTimeSec=parseInt(conf.get(TOPOLOGY_RUNTIMESEC));
      metrics(client, _messageSize, _pollFreqSec, _testRunTimeSec);
    } finally {
      //Kill it right now!!!
      KillOptions killOpts = new KillOptions();
      killOpts.set_wait_secs(0);

      for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
        LOG.info("KILLING "+_name+"_"+topoNum);
        try {
          client.killTopologyWithOpts(_name+"_"+topoNum, killOpts);
        } catch (Exception e) {
          LOG.error("Error tying to kill "+_name+"_"+topoNum,e);
        }
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    new Main().realMain(args);
  }
}
