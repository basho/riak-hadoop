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

import org.codehaus.jackson.annotate.JsonCreator;

/**
 * Models a bucket/key location in Riak.
 * 
 * @author russell
 * 
 */
public class BucketKey {

    private final String bucket;
    private final String key;

    /**
     * Provide a JSON constructor for Jackson.
     * 
     * @param bucketKey
     *            a String[2] where [0] is the bucket and [1] is the key
     */
    @JsonCreator public BucketKey(String[] bucketKey) {
        if (bucketKey == null || bucketKey.length != 2) {
            throw new IllegalArgumentException("bucketKey must be a String[] of length 2");
        }

        this.bucket = bucketKey[0];
        this.key = bucketKey[1];
    }

    /**
     * Default constructor
     * 
     * @param bucket
     *            the bucket
     * @param key
     *            the key
     */
    public BucketKey(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    /**
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * @return the key
     */
    public String getKey() {
        return key;
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
        result = prime * result + ((key == null) ? 0 : key.hashCode());
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
        if (!(obj instanceof BucketKey)) {
            return false;
        }
        BucketKey other = (BucketKey) obj;
        if (bucket == null) {
            if (other.bucket != null) {
                return false;
            }
        } else if (!bucket.equals(other.bucket)) {
            return false;
        }
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("BucketKey [bucket=%s, key=%s]", bucket, key);
    }

}
