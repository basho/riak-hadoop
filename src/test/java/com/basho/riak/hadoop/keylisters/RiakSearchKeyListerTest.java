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

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.SearchMapReduce;
import com.basho.riak.client.query.functions.Args;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.hadoop.BucketKey;
import com.basho.riak.hadoop.keylisters.RiakSearchKeyLister;

/**
 * @author russell
 * 
 */
public class RiakSearchKeyListerTest {

    private static final String BUCKET = "bucket";
    private static final String QUERY = "foo:zero";

    @Mock private IRiakClient riakClient;
    @Mock private SearchMapReduce searchMapReduce;
    @Mock private MapReduceResult mapReduceResult;

    private RiakSearchKeyLister lister;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.RiakSearchKeyLister#RiakSearchKeyLister(java.lang.String, java.lang.String)}
     * .
     */
    @Test public void createWithBucketAndQuery() throws Exception {
        lister = new RiakSearchKeyLister(BUCKET, QUERY);
        testLister(lister);
    }

    private void testLister(RiakSearchKeyLister lister) throws Exception {
        final Collection<BucketKey> expected = Arrays.asList(new BucketKey(BUCKET, "k1"), new BucketKey(BUCKET, "k2"));

        when(riakClient.mapReduce(BUCKET, QUERY)).thenReturn(searchMapReduce);
        when(searchMapReduce.addReducePhase(NamedErlangFunction.REDUCE_IDENTITY, Args.REDUCE_PHASE_ONLY_1)).thenReturn(searchMapReduce);
        when(searchMapReduce.execute()).thenReturn(mapReduceResult);
        when(mapReduceResult.getResult(BucketKey.class)).thenReturn(expected);

        final Collection<BucketKey> actual = lister.getKeys(riakClient);
        assertEquals(expected, actual);
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.RiakSearchKeyLister#RiakSearchKeyLister()}.
     */
    @Test public void emptyListerIllegalState() throws Exception {
        lister = new RiakSearchKeyLister();

        try {
            lister.getKeys(riakClient);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            // NO-OP
        }
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.RiakSearchKeyLister#getInitString()}.
     */
    @Test public void getInitString() throws Exception {
        lister = new RiakSearchKeyLister(BUCKET, QUERY);

        String initString = lister.getInitString();

        RiakSearchKeyLister listerToo = new RiakSearchKeyLister();
        listerToo.init(initString);

        testLister(listerToo);
    }
}
