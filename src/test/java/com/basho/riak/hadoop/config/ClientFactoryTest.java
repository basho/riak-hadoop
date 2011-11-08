/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.hadoop.config;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.basho.riak.client.RiakException;

/**
 * @author russell
 * 
 */
public class ClientFactoryTest {

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {}

    @Test(expected = IllegalArgumentException.class) public void getClusterClient_die() throws RiakException {
        Configuration conf = new Configuration();

        conf = RiakConfig.addLocation(conf, new RiakPBLocation("33.33.33.12", 8087));
        conf = RiakConfig.addLocation(conf, new RiakPBLocation("33.33.33.13", 8087));
        conf = RiakConfig.addLocation(conf, new RiakHTTPLocation("33.33.33.10", 8098, "riak"));
        conf = RiakConfig.addLocation(conf, new RiakHTTPLocation("33.33.33.11", 8098, "riak"));

        ClientFactory.clusterClient(RiakConfig.getRiakLocatons(conf));
    }
}
