/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import java.util.*;

class MongosHAServerSelector implements ServerSelector {
    private ServerAddress stickTo;
    private Set<ServerAddress> consideredServers = new HashSet<ServerAddress>();

    @Override
    public List<ServerDescription> choose(final ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() != ClusterConnectionMode.Multiple
            || clusterDescription.getType() != ClusterType.Sharded) {
            throw new IllegalArgumentException("This is not a sharded cluster with multiple mongos servers");
        }

        Set<ServerAddress> okServers = getOkServers(clusterDescription);

        synchronized (this) {
            if (!consideredServers.containsAll(okServers) || !okServers.contains(stickTo)) {
                if (stickTo != null && !okServers.contains(stickTo)) {
                    stickTo = null;
                    consideredServers.clear();
                }

                /*
                * We don't want the fastest server rather a random one.
                * This is due to when a single server is the fastest it will receive
                * all the connections and choke. (This is fixed better in version 3).
                ServerDescription fastestServer = null;
                for (ServerDescription cur : clusterDescription.getAny()) {
                    if (fastestServer == null || cur.getAverageLatencyNanos() < fastestServer.getAverageLatencyNanos()) {
                        fastestServer = cur;
                    }
                }
                */
                List<ServerDescription> availableServers = clusterDescription.getAny();
                if (availableServers.size() > 0) {
                    ServerDescription randomServer = availableServers.get(new Random().nextInt(availableServers.size()));
                    stickTo = randomServer.getAddress();
                    consideredServers.addAll(okServers);
                }

            }
            if (stickTo == null) {
                return Collections.emptyList();
            }
            return Arrays.asList(clusterDescription.getByServerAddress(stickTo));
        }
    }

    @Override
    public String toString() {
        return "MongosHAServerSelector{"
               + (stickTo == null ? "" : "stickTo=" + stickTo)
               + '}';
    }

    private Set<ServerAddress> getOkServers(final ClusterDescription clusterDescription) {
        Set<ServerAddress> okServers = new HashSet<ServerAddress>();
        for (ServerDescription cur : clusterDescription.getAny()) {
            okServers.add(cur.getAddress());
        }
        return okServers;
    }
}
