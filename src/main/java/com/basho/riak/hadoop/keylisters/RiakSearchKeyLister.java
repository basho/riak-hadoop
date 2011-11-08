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

import java.io.IOException;
import java.util.Collection;

import org.codehaus.jackson.map.ObjectMapper;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.functions.Args;
import com.basho.riak.client.query.functions.NamedErlangFunction;
import com.basho.riak.hadoop.BucketKey;

/**
 * Uses a Riak Search M/R query to produce a list of {@link BucketKey}s for a
 * hadoop M/R job
 * 
 * @author russell
 * 
 */
public class RiakSearchKeyLister implements KeyLister {

    private static final ObjectMapper OM = new ObjectMapper();

    private String bucket;
    private String searchQuery;

    /**
     * Create a key lister that will execute <code>searchQuery</code> for
     * <code>bucket</code> to get a list of {@link BucketKey}s
     * 
     * @param bucket
     * @param searchQuery
     */
    public RiakSearchKeyLister(String bucket, String searchQuery) {
        this.bucket = bucket;
        this.searchQuery = searchQuery;
    }

    public RiakSearchKeyLister() {}

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.KeyLister#getInitString()
     */
    public String getInitString() throws IOException {
        return OM.writeValueAsString(new String[] { bucket, searchQuery });
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.KeyLister#init(java.lang.String)
     */
    public void init(String initString) throws IOException {
        String[] bq = OM.readValue(initString, String[].class);
        bucket = bq[0];
        searchQuery = bq[1];
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.hadoop.KeyLister#getKeys(com.basho.riak.client.IRiakClient
     * )
     */
    public Collection<BucketKey> getKeys(IRiakClient client) throws RiakException {
        if (bucket == null || searchQuery == null) {
            throw new IllegalStateException("bucket and query cannot be null");
        }

        MapReduceResult result = client.mapReduce(bucket, searchQuery).addReducePhase(NamedErlangFunction.REDUCE_IDENTITY,
                                                                                      Args.REDUCE_PHASE_ONLY_1).execute();

        return result.getResult(BucketKey.class);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bucket == null) ? 0 : bucket.hashCode());
        result = prime * result + ((searchQuery == null) ? 0 : searchQuery.hashCode());
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
        if (!(obj instanceof RiakSearchKeyLister)) {
            return false;
        }
        RiakSearchKeyLister other = (RiakSearchKeyLister) obj;
        if (bucket == null) {
            if (other.bucket != null) {
                return false;
            }
        } else if (!bucket.equals(other.bucket)) {
            return false;
        }
        if (searchQuery == null) {
            if (other.searchQuery != null) {
                return false;
            }
        } else if (!searchQuery.equals(other.searchQuery)) {
            return false;
        }
        return true;
    }
}
