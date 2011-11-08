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

import static org.junit.Assert.*;

import java.util.Collection;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that the object mapper can turn [["b", "k"], ["b", "k1"]] into a
 * Collection of {@link BucketKey}
 * 
 * @author russell
 * 
 */
public class BucketKeyTest {

    /**
     * @throws java.lang.Exception
     */
    @Before public void setUp() throws Exception {}

    /**
     * Test method for
     * {@link com.basho.riak.hadoop.BucketKey#BucketKey(java.lang.String[])}.
     */
    @Test public void bucketKeyFromReduceIdentity() throws Exception {
        final String mrOut = "[[\"indexed\",\"qbert\"],[\"indexed\",\"bert\"]]";

        Collection<BucketKey> bks = new ObjectMapper().readValue(mrOut, 
                                                                 TypeFactory.collectionType(Collection.class,BucketKey.class));

        assertEquals(2, bks.size());

        assertTrue(bks.contains(new BucketKey("indexed", "qbert")));
        assertTrue(bks.contains(new BucketKey("indexed", "bert")));
    }

}
