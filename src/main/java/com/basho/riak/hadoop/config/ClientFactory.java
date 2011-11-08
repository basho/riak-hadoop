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
package com.basho.riak.hadoop.config;

import java.io.IOException;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.config.Configuration;
import com.basho.riak.client.raw.http.HTTPClientAdapter;
import com.basho.riak.client.raw.http.HTTPClientConfig;
import com.basho.riak.client.raw.http.HTTPClusterConfig;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;

/**
 * Used for generating clients for input/output
 * 
 * Replace with existing RJC factory when {@link RiakLocation}s is swapped for
 * {@link Configuration}
 * 
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

    /**
     * Generate a cluster client from an array of {@link RiakLocation}s
     * 
     * @param riakLocatons
     * @return
     * @throws IllegalArgumentException
     *             if locations are not all of same {@link RiakTransport}
     */
    public static IRiakClient clusterClient(RiakLocation[] riakLocatons) throws RiakException {
        IRiakClient client = null;
        RiakTransport transport = null;

        if (riakLocatons != null && riakLocatons.length > 0) {
            transport = riakLocatons[0].getTransport();
        }

        if (RiakTransport.PB.equals(transport)) {
            client = pbClusterClient(riakLocatons);
        } else if (RiakTransport.HTTP.equals(transport)) {
            client = httpClusterClient(riakLocatons);
        }

        return client;
    }

    /**
     * @param riakLocatons
     * @return a cluster client of HTTP clients
     */
    private static IRiakClient httpClusterClient(RiakLocation[] riakLocatons) throws RiakException {
        HTTPClusterConfig conf = new HTTPClusterConfig(500); // TODO make this config

        for (RiakLocation loc : riakLocatons) {
            if(!RiakTransport.HTTP.equals(loc.getTransport())) {
                throw new IllegalArgumentException("Cluster clients must be homogenous");
            }

            RiakHTTPLocation httpLoc = (RiakHTTPLocation)loc;
            conf.addClient(new HTTPClientConfig.Builder()
                .withHost(httpLoc.getHost())
                .withPort(httpLoc.getPort())
                .withRiakPath(httpLoc.getRiakPath())
                .build());
        }
        return RiakFactory.newClient(conf);
    }

    /**
     * @param riakLocatons
     * @return a cluster client of PB clients
     */
    private static IRiakClient pbClusterClient(RiakLocation[] riakLocatons) throws RiakException {
        PBClusterConfig conf = new PBClusterConfig(500); // TODO make this config
        
        for (RiakLocation loc : riakLocatons) {
            if(!RiakTransport.PB.equals(loc.getTransport())) {
                throw new IllegalArgumentException("Cluster clients must be homogenous");
            }
            conf.addClient(new PBClientConfig.Builder()
                .withHost(loc.getHost())
                .withPort(loc.getPort())
                .build());
        }
        return RiakFactory.newClient(conf);
    }
}
