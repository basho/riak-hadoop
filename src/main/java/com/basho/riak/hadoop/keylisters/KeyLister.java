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

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.hadoop.BucketKey;

/**
 * Strategy for obtaining list of keys for splits, {@link KeyLister}s must a
 * zero arg constructor.
 * 
 * @author russell
 * 
 */
public interface KeyLister {
    /**
     * Thanks to hadoop's configuration framework a key lister has to
     * deserialize and serialize itself this method and init(String) below are a
     * light weight way of doing that
     * 
     * @return a String that can be used by the implementations init method to
     *         reconsitute the state of the lister
     * @throws IOException
     */
    String getInitString() throws IOException;

    /**
     * A string (from a prior call to getInitString) that this instance will use
     * to set itself up to list keys
     * 
     * @param initString
     * @throws IOException
     */
    void init(String initString) throws IOException;

    /**
     * Get keys with the given client
     * 
     * @param client
     * @return
     * @throws RiakException
     * @throws {@link IllegalStateException} is init was not called and the
     *         lister is not set up to get keys
     */
    Collection<BucketKey> getKeys(IRiakClient client) throws RiakException;
}
