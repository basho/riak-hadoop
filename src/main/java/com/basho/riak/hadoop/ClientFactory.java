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

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.http.HTTPClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientAdapter;

/**
 * @author russell
 * 
 */
public final class ClientFactory {

    private ClientFactory() {}

    public static IRiakClient getClient(RiakLocation location) throws RiakException {
        // TODO this should use getRawClient, but DefaultRiakClient's
        // constructor is wrong visibility
        // Either change the visibility or add a method to the factory to accept
        // a delegate (the latter!)
        IRiakClient client = null;
        switch (location.getTransport()) {
        case PB:
            client = RiakFactory.pbcClient(location.getHost(), location.getPort());
            break;
        case HTTP:
            client = RiakFactory.httpClient(location.asString());
            break;
        default:
            throw new RiakException("Unknown Transport");
        }
        return client;
    }

    public static RawClient getRawClient(RiakLocation location) throws IOException {
        RawClient client = null;
        switch (location.getTransport()) {
        case PB:
            client = new PBClientAdapter(location.getHost(), location.getPort());
            break;
        case HTTP:
            client = new HTTPClientAdapter(location.asString());
            break;
        default:
            throw new IOException("Unknown Transport");
        }
        return client;
    }
}
