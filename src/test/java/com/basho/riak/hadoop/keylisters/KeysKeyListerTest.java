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
package com.basho.riak.hadoop.keylisters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.basho.riak.hadoop.BucketKey;
import com.basho.riak.hadoop.keylisters.KeysKeyLister;

/**
 * @author russell
 * 
 */
public class KeysKeyListerTest {

    private static final String BUCKET_NAME = "bucket";

    private KeysKeyLister lister;

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.KeysKeyLister#KeysKeyLister(java.util.List)}
     * .
     */
    @Test public void createWithKeys() throws Exception {
        Set<BucketKey> keys = new HashSet<BucketKey>(Arrays.asList(new BucketKey(BUCKET_NAME, "k1"), new BucketKey(BUCKET_NAME,
                                                                                                        "k2s")));
        lister = new KeysKeyLister(keys);
        assertEquals(keys, lister.getKeys(null));
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.KeysKeyLister#KeysKeyLister(java.util.List, java.lang.String)}
     * .
     */
    @Test public void createWithKeysAndCommonBucket() throws Exception {
        Set<String> keys = new HashSet<String>(Arrays.asList("k1", "k2", "k3", "k4"));
        lister = new KeysKeyLister(keys, BUCKET_NAME);

        Set<BucketKey> expected = new HashSet<BucketKey>();
        for (String k : keys) {
            expected.add(new BucketKey(BUCKET_NAME, k));
        }

        assertEquals(expected, lister.getKeys(null));
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.KeysKeyLister#KeysKeyLister()}.
     */
    @Test public void noArgConstructorAndNoInitMeansIllegalState() throws Exception {
        lister = new KeysKeyLister();

        try {
            lister.getKeys(null);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            // NO-OP
        }

    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.KeysKeyLister#getInitString()}.
     */
    @Test public void initFromString() throws Exception {
        List<BucketKey> keys = Arrays.asList(new BucketKey(BUCKET_NAME, "k1"), new BucketKey(BUCKET_NAME, "k2s"));
        lister = new KeysKeyLister(keys);

        KeysKeyLister lister2 = new KeysKeyLister();
        lister2.init(lister.getInitString());

        assertEquals(lister.getKeys(null), lister2.getKeys(null));
    }
}
