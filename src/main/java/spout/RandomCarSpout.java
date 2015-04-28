/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomCarSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    

    /**
     * car info format: 
     * region_name carID time lat lon speed heading
     *  
     * region_name should later be preprocessed and used to detemine which bolt to send the tuple
     * heading is assumed to be integer
     */
    String[] sentences = new String[]{ 
    		"Clementi 1 103.74792 1.35008 11/06/2008 00:00:00 40 1", 
    		"AMK 1 103.74792 1.35008 11/06/2008 00:00:00 40 1",
            "Clementi 1 103.74792 1.35008 11/06/2008 00:00:00 40 1", 
            "AMK 1 103.74792 1.35008 11/06/2008 00:00:00 40 1", 
            "Simei 1 103.74792 1.35008 11/06/2008 00:00:00 40 1" };
    
    String sentence = sentences[_rand.nextInt(sentences.length)];
    int idx = sentence.indexOf(" ");
    String regionName = sentence.substring(0, idx); 
    _collector.emit(new Values(regionName, sentence));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("region_name", "record"));
  }

}