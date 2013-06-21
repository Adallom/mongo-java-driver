/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.mongodb

import org.mongodb.Document
import org.mongodb.codecs.DocumentCodec
import org.mongodb.command.MongoCommandFailureException
import org.mongodb.command.Ping
import org.mongodb.connection.Cluster
import org.mongodb.session.ServerSelectingSession
import spock.lang.Specification
import spock.lang.Subject

import static com.mongodb.ReadPreference.primary

class DBSpecification extends Specification {
    private final ServerSelectingSession session = Mock()
    private final Cluster cluster = Mock()
    private final Mongo mongo = Mock()

    @Subject
    private final DB database = new DB(mongo, 'myDatabase', new DocumentCodec())

    def setup() {
        mongo.getCluster() >> { cluster }
        mongo.getSession() >> { session }

        //TODO: this shouldn't be required.  I think.
        database.setReadPreference(primary())
    }

    @SuppressWarnings('UnnecessaryQualifiedReference')
    def 'should throw com.mongodb.MongoException if createCollection fails'() {
        setup:
        session.execute(_) >> {
            throw new MongoCommandFailureException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                           new org.mongodb.connection.ServerAddress(),
                                                                                           new Document(),
                                                                                           15L))
        }

        when:
        database.createCollection('myNewCollection', new BasicDBObject());

        then:
        thrown(com.mongodb.MongoException)
    }

    @SuppressWarnings('UnnecessaryQualifiedReference')
    def 'should throw com.mongodb.MongoException if executeCommand fails'() {
        setup:
        session.execute(_) >> {
            throw new MongoCommandFailureException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                           new org.mongodb.connection.ServerAddress(),
                                                                                           new Document(),
                                                                                           15L))
        }

        when:
        database.executeCommand(new Ping());

        then:
        thrown(MongoCommandFailureException)
    }

    //TODO: getCollectionNames declares an exception, but doesn't wrap one
}
