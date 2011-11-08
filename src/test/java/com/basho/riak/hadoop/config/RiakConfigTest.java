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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.raw.query.indexes.BinRangeQuery;
import com.basho.riak.hadoop.config.RiakConfig;
import com.basho.riak.hadoop.config.RiakHTTPLocation;
import com.basho.riak.hadoop.config.RiakLocation;
import com.basho.riak.hadoop.config.RiakPBLocation;
import com.basho.riak.hadoop.keylisters.BucketKeyLister;
import com.basho.riak.hadoop.keylisters.KeyLister;
import com.basho.riak.hadoop.keylisters.KeysKeyLister;
import com.basho.riak.hadoop.keylisters.RiakSearchKeyLister;
import com.basho.riak.hadoop.keylisters.SecondaryIndexesKeyLister;

/**
 * @author russell
 * 
 */
public class RiakConfigTest {

    private static final String BUCKET = "bucket";

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.config.RiakConfig#addLocation(org.apache.hadoop.conf.Configuration, com.basho.riak.hadoop.config.RiakLocation)}
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
     * {@link com.basho.riak.hadoop.config.RiakConfig#getRiakLocatons(org.apache.hadoop.conf.Configuration)}
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

    @Test public void setAndGetKeyLister() throws Exception {
        Configuration conf = new Configuration();

        BucketKeyLister bkl = new BucketKeyLister(BUCKET);
        conf = RiakConfig.setKeyLister(conf, bkl);
        KeyLister actual = RiakConfig.getKeyLister(conf);
        assertEquals(bkl, actual);

        KeysKeyLister kkl = new KeysKeyLister(Arrays.asList("k1", "k2", "k3", "k4"), BUCKET);
        conf = RiakConfig.setKeyLister(conf, kkl);
        actual = RiakConfig.getKeyLister(conf);
        assertEquals(kkl, actual);

        RiakSearchKeyLister rskl = new RiakSearchKeyLister(BUCKET, "foo:zero");
        conf = RiakConfig.setKeyLister(conf, rskl);
        actual = RiakConfig.getKeyLister(conf);
        assertEquals(rskl, actual);

        SecondaryIndexesKeyLister sikl = new SecondaryIndexesKeyLister(new BinRangeQuery(BinIndex.named("twitter"),
                                                                                         BUCKET, "from", "to"));
        conf = RiakConfig.setKeyLister(conf, sikl);
        actual = RiakConfig.getKeyLister(conf);
        assertEquals(sikl, actual);
    }
}
