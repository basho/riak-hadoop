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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.mapreduce.Mapper;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.raw.RiakResponse;

/**
 * @author russell
 * @param <T>
 *            the type for the input value
 * @param <OK>
 *            the type for the out key
 * @param <OV>
 *            the type for the out value
 * 
 */
public abstract class RiakMapper<T, OK, OV> extends Mapper<BucketKey, RiakResponse, OK, OV> {

    private final Converter<T> converter;
    private final ConflictResolver<T> resolver;

    /**
     * @param converter
     * @param resolver
     */
    public RiakMapper(Converter<T> converter, ConflictResolver<T> resolver) {
        this.converter = converter;
        this.resolver = resolver;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override public void map(BucketKey key, RiakResponse value, Context context) throws IOException,
            InterruptedException {

        // convert, conflict resolve
        final Collection<T> siblings = new ArrayList<T>(value.numberOfValues());

        for (IRiakObject o : value) {
            siblings.add(converter.toDomain(o));
        }

        map(key, resolver.resolve(siblings), context);
    }

    public abstract void map(BucketKey k, T value, Context context) throws IOException, InterruptedException;

}
