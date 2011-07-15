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
package com.basho.riak.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * @author russell
 * 
 */
public class RiakConfigTest {

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.RiakConfig#addLocation(org.apache.hadoop.conf.Configuration, com.basho.riak.hadoop.RiakLocation)}
     * .
     */
    @Test public void testAddRiakLocations() {
        final String host = "127.0.0.1";
        final int port = 8097;
        Configuration conf = new Configuration();
        conf = RiakConfig.addLocation(conf, new RiakPBLocation(host, port));
        conf = RiakConfig.addLocation(conf, new RiakHTTPLocation(host, port, "riak"));

        assertEquals("127.0.0.1:8097,http://127.0.0.1:8097/riak", conf.get(RiakConfig.LOCATIONS_PROPERTY));
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.RiakConfig#getRiakLocatons(org.apache.hadoop.conf.Configuration)}
     * .
     */
    @Test public void testGetRiakLocatons() {
        Configuration conf = new Configuration();
        conf.set(RiakConfig.LOCATIONS_PROPERTY, "127.0.0.1:8097,http://127.0.0.1:8097/riak");

        RiakLocation[] locations = RiakConfig.getRiakLocatons(conf);

        assertEquals(2, locations.length);
        assertTrue(locations[0] instanceof RiakPBLocation);
        assertTrue(locations[1] instanceof RiakHTTPLocation);
        assertEquals("127.0.0.1:8097", locations[0].asString());
        assertEquals("http://127.0.0.1:8097/riak", locations[1].asString());
    }
}
