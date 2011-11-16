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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.Args;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.client.query.indexes.BinIndex;
import com.basho.riak.client.query.indexes.IntIndex;
import com.basho.riak.client.raw.query.indexes.BinRangeQuery;
import com.basho.riak.client.raw.query.indexes.BinValueQuery;
import com.basho.riak.client.raw.query.indexes.IndexQuery;
import com.basho.riak.client.raw.query.indexes.IndexWriter;
import com.basho.riak.client.raw.query.indexes.IntRangeQuery;
import com.basho.riak.client.raw.query.indexes.IntValueQuery;
import com.basho.riak.hadoop.BucketKey;

/**
 * Uses a 2i query to get keys for hadoop m/r.
 * 
 * @author russell
 * 
 */
public class SecondaryIndexesKeyLister implements KeyLister {
    private static final String BUCKET = "bucket";
    private static final String INDEX = "index";
    private static final String KEY = "key";
    private static final String START = "start";
    private static final String END = "end";

    private IndexQuery query;

    /**
     * @param query
     */
    public SecondaryIndexesKeyLister(IndexQuery query) {
        this.query = query;
    }

    public SecondaryIndexesKeyLister() {}

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.KeyLister#getInitString()
     */
    public String getInitString() throws IOException {
        // TODO, this is the same as the code in IndexMapReduce, abstract out to
        // common class
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        final JsonGenerator jg = new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8);

        jg.writeStartObject();

        IndexWriter e = new IndexWriter() {

            private void writeCommon(String bucket, String index) throws IOException {
                jg.writeStringField(BUCKET, bucket);
                jg.writeStringField(INDEX, index);
            }

            public void write(String bucket, String index, int from, int to) throws IOException {
                writeCommon(bucket, index);
                jg.writeNumberField(START, from);
                jg.writeNumberField(END, to);
            }

            public void write(String bucket, String index, int value) throws IOException {
                writeCommon(bucket, index);
                jg.writeNumberField(KEY, value);
            }

            public void write(String bucket, String index, String from, String to) throws IOException {
                writeCommon(bucket, index);
                jg.writeStringField(START, from);
                jg.writeStringField(END, to);
            }

            public void write(String bucket, String index, String value) throws IOException {
                writeCommon(bucket, index);
                jg.writeStringField(KEY, value);
            }
        };

        query.write(e);
        jg.writeEndObject();
        jg.flush();
        jg.close();
        return out.toString("UTF-8");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.KeyLister#init(java.lang.String)
     */
    public void init(String initString) throws IOException {
        // just like FetchIndex, again, abstract out to a common class
        boolean isRange = false;
        // turn the Json into an index query
        @SuppressWarnings("rawtypes") Map map = new ObjectMapper().readValue(initString, Map.class);

        String indexName = (String) map.get(INDEX);
        String bucket = (String) map.get(BUCKET);
        Object value = map.get(KEY);
        Object from = map.get(START);
        Object to = map.get(END);

        if (indexName == null) {
            throw new IllegalArgumentException("no index present");
        }
        if (from != null && to != null && value == null) {
            isRange = true;
        }

        if (indexName != null && indexName.endsWith("_int")) {
            if (isRange) {
                query = new IntRangeQuery(IntIndex.named(indexName), bucket, (Integer) from, (Integer) to);
            } else {
                query = new IntValueQuery(IntIndex.named(indexName), bucket, (Integer) value);
            }
        }

        if (indexName != null && indexName.endsWith("_bin")) {
            if (isRange) {
                query = new BinRangeQuery(BinIndex.named(indexName), bucket, (String) from, (String) to);
            } else {
                query = new BinValueQuery(BinIndex.named(indexName), bucket, (String) value);
            }
        }

        if (query == null) {
            throw new IOException("unable to parse query from init string");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.hadoop.KeyLister#getKeys(com.basho.riak.client.IRiakClient
     * )
     */
    public Collection<BucketKey> getKeys(IRiakClient client) throws RiakException {
        if (query == null) {
            throw new IllegalStateException("No index query");
        }
        MapReduceResult r = client.mapReduce(query).addReducePhase(NamedErlangFunction.REDUCE_IDENTITY,
                                                                   Args.REDUCE_PHASE_ONLY_1).execute();

        return r.getResult(BucketKey.class);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((query == null) ? 0 : query.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof SecondaryIndexesKeyLister)) {
            return false;
        }
        SecondaryIndexesKeyLister other = (SecondaryIndexesKeyLister) obj;
        if (query == null) {
            if (other.query != null) {
                return false;
            }
        } else if (!query.equals(other.query)) {
            return false;
        }
        return true;
    }
}
