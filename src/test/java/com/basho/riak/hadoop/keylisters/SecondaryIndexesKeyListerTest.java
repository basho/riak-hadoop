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
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.query.IndexMapReduce;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.Args;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;
import com.basho.riak.client.raw.query.indexes.BinRangeQuery;
import com.basho.riak.client.raw.query.indexes.BinValueQuery;
import com.basho.riak.client.raw.query.indexes.IndexQuery;
import com.basho.riak.client.raw.query.indexes.IntRangeQuery;
import com.basho.riak.client.raw.query.indexes.IntValueQuery;
import com.basho.riak.hadoop.BucketKey;
import com.basho.riak.hadoop.keylisters.SecondaryIndexesKeyLister;

/**
 * @author russell
 * 
 */
public class SecondaryIndexesKeyListerTest {

    private static final String INDEX = "index";
    private static final String BUCKET = "bucket";
    private static final String VALUE = "value";
    private static final String FROM = "from";
    private static final String TO = "to";

    @Mock private IRiakClient riakClient;
    @Mock private IndexMapReduce indexMapReduce;
    @Mock private MapReduceResult result;

    private SecondaryIndexesKeyLister lister;

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.SecondaryIndexesKeyLister#SecondaryIndexesKeyLister(com.basho.riak.client.raw.query.indexes.IndexQuery)}
     * .
     */
    @Test public void constructWithQuery() throws Exception {

        IndexQuery query = new BinRangeQuery(BinIndex.named(INDEX), BUCKET, FROM, TO);
        lister = new SecondaryIndexesKeyLister(query);

        testLister(lister, query);
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.SecondaryIndexesKeyLister#SecondaryIndexesKeyLister()}
     * .
     */
    @Test public void illegalState() throws Exception {
        lister = new SecondaryIndexesKeyLister();

        try {
            lister.getKeys(riakClient);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            // NO-OP
        }
    }

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.keylisters.SecondaryIndexesKeyLister#getInitString()}.
     */
    @Test public void getInitString_binRange() throws Exception {
        IndexQuery query = new BinRangeQuery(BinIndex.named(INDEX), BUCKET, FROM, TO);
        lister = new SecondaryIndexesKeyLister(query);

        String initString = lister.getInitString();

        SecondaryIndexesKeyLister listerToo = new SecondaryIndexesKeyLister();
        listerToo.init(initString);

        testLister(listerToo, query);
    }

    @Test public void getInitString_binValue() throws Exception {
        IndexQuery query = new BinValueQuery(BinIndex.named(INDEX), BUCKET, VALUE);
        lister = new SecondaryIndexesKeyLister(query);

        String initString = lister.getInitString();

        SecondaryIndexesKeyLister listerToo = new SecondaryIndexesKeyLister();
        listerToo.init(initString);

        testLister(listerToo, query);
    }

    @Test public void getInitString_intnRange() throws Exception {
        IndexQuery query = new IntRangeQuery(IntIndex.named(INDEX), BUCKET, 1, 100);
        lister = new SecondaryIndexesKeyLister(query);

        String initString = lister.getInitString();

        SecondaryIndexesKeyLister listerToo = new SecondaryIndexesKeyLister();
        listerToo.init(initString);

        testLister(listerToo, query);
    }

    @Test public void getInitString_intValue() throws Exception {
        IndexQuery query = new IntValueQuery(IntIndex.named(INDEX), BUCKET, 10);
        lister = new SecondaryIndexesKeyLister(query);

        String initString = lister.getInitString();

        SecondaryIndexesKeyLister listerToo = new SecondaryIndexesKeyLister();
        listerToo.init(initString);

        testLister(listerToo, query);
    }

    private void testLister(SecondaryIndexesKeyLister lister, IndexQuery query) throws Exception {
        final Collection<BucketKey> expected = Arrays.asList(new BucketKey(BUCKET, "k1"), new BucketKey(BUCKET, "k2"));
        when(riakClient.mapReduce(query)).thenReturn(indexMapReduce);
        when(indexMapReduce.addReducePhase(NamedErlangFunction.REDUCE_IDENTITY, Args.REDUCE_PHASE_ONLY_1)).thenReturn(indexMapReduce);
        when(indexMapReduce.execute()).thenReturn(result);
        when(result.getResult(BucketKey.class)).thenReturn(expected);

        Collection<BucketKey> actual = lister.getKeys(riakClient);

        assertEquals(expected, actual);
    }
}
