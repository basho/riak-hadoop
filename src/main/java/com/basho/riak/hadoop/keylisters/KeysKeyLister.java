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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.hadoop.BucketKey;

/**
 * Key lister that simply returns the list of keys it is configured with.
 * 
 * If you get your key list from outside Riak, or for testing a subset of data.
 * 
 * @author russell
 * 
 */
public class KeysKeyLister implements KeyLister {

    private static final String BK_SEPARATOR = ":";
    private static final String ENTRY_SEPARATOR = ",";

    private Set<BucketKey> keys = null;

    /**
     * Provide the keys directly (don't look up in Riak)
     * 
     * @param keys
     *            the keys to M/R over
     */
    public KeysKeyLister(Collection<BucketKey> keys) {
        this.keys = new HashSet<BucketKey>(keys);
    }

    /**
     * Provide the keys directly (don't look up in Riak)
     * 
     * @param keys
     *            the keys to M/R over
     * @param bucket
     *            a common bucket the keys share
     */
    public KeysKeyLister(Collection<String> keys, String bucket) {
        this.keys = new HashSet<BucketKey>();
        for (String k : keys) {
            this.keys.add(new BucketKey(bucket, k));
        }
    }

    public KeysKeyLister() {};

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.KeyLister#getInitString()
     */
    public String getInitString() {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (BucketKey bk : keys) {
            sb.append(sep).append(bk.getBucket()).append(BK_SEPARATOR).append(bk.getKey());
            sep = ENTRY_SEPARATOR;
        }

        return sb.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.hadoop.KeyLister#init(java.lang.String)
     */
    public void init(String initString) {
        if (initString == null) {
            throw new IllegalArgumentException("initString cannot be null");
        }
        this.keys = new HashSet<BucketKey>();
        String[] bks = initString.split(ENTRY_SEPARATOR);

        for (String bk : bks) {
            String[] bucketKey = bk.split(BK_SEPARATOR);
            keys.add(new BucketKey(bucketKey[0], bucketKey[1]));
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
        if (keys == null) {
            throw new IllegalStateException("lister not initialised");
        }
        return new HashSet<BucketKey>(keys);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((keys == null) ? 0 : keys.hashCode());
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
        if (!(obj instanceof KeysKeyLister)) {
            return false;
        }
        KeysKeyLister other = (KeysKeyLister) obj;
        if (keys == null) {
            if (other.keys != null) {
                return false;
            }
        } else if (!keys.equals(other.keys)) {
            return false;
        }
        return true;
    }

}
