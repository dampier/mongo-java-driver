// ReplicaSetStatus.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import org.bson.util.annotations.Immutable;
import org.bson.util.annotations.ThreadSafe;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO:
//  pull config to get
//  priority
//  slave delay

/**
 * Keeps replica set status.  Maintains a background thread to ping all members of the set to keep the status current.
 */
@ThreadSafe
public class ReplicaSetStatus extends ConnectionStatus {

    static final Logger _rootLogger = Logger.getLogger( "com.mongodb.ReplicaSetStatus" );

    ReplicaSetStatus( Mongo mongo, List<ServerAddress> initial ){
        super(initial, mongo);
        _updater = new Updater(initial);
    }

    public String getName() {
        return _replicaSetHolder.get().getSetName();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{replSetName: ").append(_replicaSetHolder.get().getSetName());
        sb.append(", members: ").append(_replicaSetHolder);
        sb.append(", updaterIntervalMS: ").append(updaterIntervalMS);
        sb.append(", updaterIntervalNoMasterMS: ").append(updaterIntervalNoMasterMS);
        sb.append(", slaveAcceptableLatencyMS: ").append(slaveAcceptableLatencyMS);
        sb.append(", inetAddrCacheMS: ").append(inetAddrCacheMS);
        sb.append(", latencySmoothFactor: ").append(latencySmoothFactor);
        sb.append("}");

        return sb.toString();
    }

    /**
     * @return master or null if don't have one
     * @throws MongoException
     */
    public ServerAddress getMaster(){
        ReplicaSetNode n = getMasterNode();
        if ( n == null )
            return null;
        return n.getServerAddress();
    }

    ReplicaSetNode getMasterNode(){
        checkClosed();
        return _replicaSetHolder.get().getMaster();
    }

    /**
     * @param srv the server to compare
     * @return indication if the ServerAddress is the current Master/Primary
     * @throws MongoException
     */
    public boolean isMaster(ServerAddress srv) {
        if (srv == null)
            return false;

	return srv.equals(getMaster());
    }

    /**
     * @return a good secondary or null if can't find one
     */
    ServerAddress getASecondary() {
        ReplicaSetNode node = _replicaSetHolder.get().getASecondary();
        if (node == null) {
            return null;
        }
        return node._addr;
    }

    @Override
    boolean hasServerUp() {
        for (ReplicaSetNode node : _replicaSetHolder.get().getAll()) {
            if (node.isOk()) {
                return true;
            }
        }
        return false;
    }

    // Simple abstraction over a volatile ReplicaSet reference that starts as null.  The get method blocks until members
    // is not null. The set method notifies all, thus waking up all getters.
    @ThreadSafe
    class ReplicaSetHolder {
        private volatile ReplicaSet members;

        // blocks until replica set is set, or a timeout occurs
        synchronized ReplicaSet get() {
            while (members == null) {
                try {
                    wait(_mongo.getMongoOptions().getConnectTimeout());
                } catch (InterruptedException e) {
                    throw new MongoInterruptedException("Interrupted while waiting for next update to replica set status", e);
                }
            }
            return members;
        }

        // set the replica set to a non-null value and notifies all threads waiting.
        synchronized void set(ReplicaSet members) {
            if (members == null) {
                throw new IllegalArgumentException("members can not be null");
            }

            this.members = members;
            notifyAll();
        }

        // blocks until the replica set is set again
        synchronized void waitForNextUpdate() {
            try {
                wait(_mongo.getMongoOptions().getConnectTimeout());
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted while waiting for next update to replica set status", e);
            }
        }

        public synchronized void close() {
            this.members = null;
            notifyAll();
        }

        public String toString() {
            ReplicaSet cur = this.members;
            if (cur != null) {
                return cur.toString();
            }
            return "none";
        }
    }

    // Immutable snapshot state of a replica set. Since the nodes don't change state, this class pre-computes the list
    // of good secondaries so that choosing a random good secondary is dead simple
    @Immutable
    static class ReplicaSet {
        final List<ReplicaSetNode> all;
        final Random random;
        final List<ReplicaSetNode> goodSecondaries;
        final List<ReplicaSetNode> goodMembers;
        final ReplicaSetNode master;
        final String setName;
        final ReplicaSetErrorStatus errorStatus;

        private int acceptableLatencyMS;
        
        public ReplicaSet(List<ReplicaSetNode> nodeList, Random random, int acceptableLatencyMS) {
            
            this.random = random;
            this.all = Collections.unmodifiableList(new ArrayList<ReplicaSetNode>(nodeList));
            this.acceptableLatencyMS = acceptableLatencyMS;

            errorStatus = validate();
            setName = determineSetName();

            this.goodSecondaries =
                    Collections.unmodifiableList(calculateGoodSecondaries(all, calculateBestPingTime(all), acceptableLatencyMS));
            this.goodMembers =
                    Collections.unmodifiableList(calculateGoodMembers(all, calculateBestPingTime(all), acceptableLatencyMS));
            master = findMaster();
        }

        public List<ReplicaSetNode> getAll() {
            checkStatus();
            
            return all;
        }

        public boolean hasMaster() {
            return getMaster() != null;
        }

        public ReplicaSetNode getMaster() {
            checkStatus();
            
            return master;
        }

        public int getMaxBsonObjectSize() {
            if (hasMaster()) {
                return getMaster().getMaxBsonObjectSize();
            } else {
                return Bytes.MAX_OBJECT_SIZE;
            }
        }

        public ReplicaSetNode getASecondary() {
            checkStatus();
            
            if (goodSecondaries.isEmpty()) {
                return null;
            }
            return goodSecondaries.get(random.nextInt(goodSecondaries.size()));
        }

        public ReplicaSetNode getASecondary(List<Tag> tags) {
            checkStatus();
            
            // optimization
            if (tags.isEmpty()) {
                return getASecondary();
            }

            List<ReplicaSetNode> acceptableTaggedSecondaries = getGoodSecondariesByTags(tags);

            if (acceptableTaggedSecondaries.isEmpty()) {
                return null;
            }
            return acceptableTaggedSecondaries.get(random.nextInt(acceptableTaggedSecondaries.size()));
        }
        
        public ReplicaSetNode getAMember() {
            checkStatus();
            
            if (goodMembers.isEmpty()) {
                return null;
            }
            return goodMembers.get(random.nextInt(goodMembers.size()));
        }

        public ReplicaSetNode getAMember(List<Tag> tags) {
            checkStatus();
            
            if (tags.isEmpty())
                return getAMember();

            List<ReplicaSetNode> acceptableTaggedMembers = getGoodMembersByTags(tags);

            if (acceptableTaggedMembers.isEmpty())
                return null;
                
            return acceptableTaggedMembers.get(random.nextInt(acceptableTaggedMembers.size()));
        }

        public List<ReplicaSetNode> getGoodSecondariesByTags(final List<Tag> tags) {
            checkStatus();
            
            List<ReplicaSetNode> taggedSecondaries = getMembersByTags(all, tags);
            return calculateGoodSecondaries(taggedSecondaries,
                    calculateBestPingTime(taggedSecondaries), acceptableLatencyMS);
        }
        
        public List<ReplicaSetNode> getGoodMembersByTags(final List<Tag> tags) {
            checkStatus();
            
            List<ReplicaSetNode> taggedMembers = getMembersByTags(all, tags);
            return calculateGoodMembers(taggedMembers,
                    calculateBestPingTime(taggedMembers), acceptableLatencyMS);
        }
        
        public List<ReplicaSetNode> getGoodMembers() {            
            checkStatus();
            
            return calculateGoodMembers(all, calculateBestPingTime(all), acceptableLatencyMS);
        }
        
        public String getSetName() {
            checkStatus();
            
            return setName;
        }
        
        public ReplicaSetErrorStatus getErrorStatus(){
            return errorStatus;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[ ");
            for (ReplicaSetNode node : getAll())
                sb.append(node.toJSON()).append(",");
            sb.setLength(sb.length() - 1); //remove last comma
            sb.append(" ]");
            return sb.toString();
        }
        
        private void checkStatus(){
            if (!errorStatus.isOk())
                throw new MongoException(errorStatus.getError());
        }

        private ReplicaSetNode findMaster() {
            for (ReplicaSetNode node : all) {
                if (node.master())
                    return node;
            }
            return null;
        }
        
        private String determineSetName() {
            for (ReplicaSetNode node : all) {
                String nodeSetName = node.getSetName();
                
                if (nodeSetName != null && !nodeSetName.equals("")) {
                    return nodeSetName;
                }
            }

            return null;
        }
        
        private ReplicaSetErrorStatus validate() {
            //make sure all nodes have the same set name
            HashSet<String> nodeNames = new HashSet<String>();
            
            for(ReplicaSetNode node : all) {
                String nodeSetName = node.getSetName();
                
                if(nodeSetName != null && !nodeSetName.equals("")) {
                    nodeNames.add(nodeSetName);
                }
            }
            
            if(nodeNames.size() <= 1)
                return new ReplicaSetErrorStatus(true, null);
            else {
                return new ReplicaSetErrorStatus(false, "nodes with different set names detected: " + nodeNames.toString());
            }
        }

        static float calculateBestPingTime(List<ReplicaSetNode> members) {
            float bestPingTime = Float.MAX_VALUE;
            for (ReplicaSetNode cur : members) {
                if (!cur.secondary()) {
                    continue;
                }
                if (cur._pingTime < bestPingTime) {
                    bestPingTime = cur._pingTime;
                }
            }
            return bestPingTime;
        }

        static List<ReplicaSetNode> calculateGoodMembers(List<ReplicaSetNode> members, float bestPingTime, int acceptableLatencyMS) {
            List<ReplicaSetNode> goodSecondaries = new ArrayList<ReplicaSetNode>(members.size());
            for (ReplicaSetNode cur : members) {
                if (!cur.isOk()) {
                    continue;
                }
                if (cur._pingTime - acceptableLatencyMS <= bestPingTime ) {
                    goodSecondaries.add(cur);
                }
            }
            return goodSecondaries;
        }
        
        static List<ReplicaSetNode> calculateGoodSecondaries(List<ReplicaSetNode> members, float bestPingTime, int acceptableLatencyMS) {
            List<ReplicaSetNode> goodSecondaries = new ArrayList<ReplicaSetNode>(members.size());
            for (ReplicaSetNode cur : members) {
                if (!cur.secondary()) {
                    continue;
                }
                if (cur._pingTime - acceptableLatencyMS <= bestPingTime ) {
                    goodSecondaries.add(cur);
                }
            }
            return goodSecondaries;
        }

        static List<ReplicaSetNode> getMembersByTags(List<ReplicaSetNode> members, List<Tag> tags) {
           
            List<ReplicaSetNode> membersByTag = new ArrayList<ReplicaSetNode>();
            
            for (ReplicaSetNode cur : members) {
                if (tags != null && cur.getTags() != null && cur.getTags().containsAll(tags)) {
                    membersByTag.add(cur);
                }
            }

            return membersByTag;
        }

    }

    // Represents the state of a node in the replica set.  Instances of this class are immutable.
    @Immutable
    static class ReplicaSetNode extends Node {
        ReplicaSetNode(ServerAddress addr, Set<String> names, String setName, float pingTime, boolean ok, boolean isMaster, boolean isSecondary,
                       LinkedHashMap<String, String> tags, int maxBsonObjectSize) {
            this(addr, null, names, setName, pingTime, ok, isMaster, isSecondary, tags, maxBsonObjectSize);
        }

        ReplicaSetNode(ServerAddress addr, ServerAddress altAddr, Set<String> names, String setName, float pingTime, boolean ok, boolean isMaster, boolean isSecondary,
                       LinkedHashMap<String, String> tags, int maxBsonObjectSize) {
            super(pingTime, addr, altAddr, maxBsonObjectSize, ok);
            this._names = Collections.unmodifiableSet(new HashSet<String>(names));
            this._setName = setName;
            this._isMaster = isMaster;
            this._isSecondary = isSecondary;
            this._tags = Collections.unmodifiableSet(getTagsFromMap(tags));
        }

        private static Set<Tag> getTagsFromMap(LinkedHashMap<String,String> tagMap) {
            Set<Tag> tagSet = new HashSet<Tag>();
            for (Map.Entry<String, String> curEntry : tagMap.entrySet()) {
                tagSet.add(new Tag(curEntry.getKey(), curEntry.getValue()));
            }
            return tagSet;
        }

        public boolean master(){
            return _ok && _isMaster;
        }

        public boolean secondary(){
            return _ok && _isSecondary;
        }

        public Set<String> getNames() {
            return _names;
        }
        
        public String getSetName() {
            return _setName;
        }

        public Set<Tag> getTags() {
            return _tags;
        }

        public float getPingTime() {
            return _pingTime;
        }

        public String toJSON(){
            StringBuilder buf = new StringBuilder();
            buf.append( "{ address:'" ).append( _addr ).append( "', " );
            if ( _altAddr != null )
                buf.append( "altAddress:'" ).append( _altAddr ).append( "', " );
            buf.append( "ok:" ).append( _ok ).append( ", " );
            buf.append( "ping:" ).append( _pingTime ).append( ", " );
            buf.append( "isMaster:" ).append( _isMaster ).append( ", " );
            buf.append( "isSecondary:" ).append( _isSecondary ).append( ", " );
            buf.append( "setName:" ).append( _setName ).append( ", " );
            buf.append( "maxBsonObjectSize:" ).append( _maxBsonObjectSize ).append( ", " );
            if(_tags != null && _tags.size() > 0){
                List<DBObject> tagObjects = new ArrayList<DBObject>();
                for( Tag tag : _tags)
                    tagObjects.add(tag.toDBObject());
                
                buf.append(new BasicDBObject("tags", tagObjects) );
            }
                
            buf.append("}");

            return buf.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ReplicaSetNode node = (ReplicaSetNode) o;

            if (_isMaster != node._isMaster) return false;
            if (_maxBsonObjectSize != node._maxBsonObjectSize) return false;
            if (_isSecondary != node._isSecondary) return false;
            if (_ok != node._ok) return false;
            if (Float.compare(node._pingTime, _pingTime) != 0) return false;
            if (!isHavingSameAddrs(node)) return false;
            if (!_names.equals(node._names)) return false;
            if (!_tags.equals(node._tags)) return false;
            if (!_setName.equals(node._setName)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = _addr.hashCode();
            result = 31 * result + (_altAddr == null ? 0 : _altAddr.hashCode());
            result = 31 * result + (_pingTime != +0.0f ? Float.floatToIntBits(_pingTime) : 0);
            result = 31 * result + _names.hashCode();
            result = 31 * result + _tags.hashCode();
            result = 31 * result + (_ok ? 1 : 0);
            result = 31 * result + (_isMaster ? 1 : 0);
            result = 31 * result + (_isSecondary ? 1 : 0);
            result = 31 * result + _setName.hashCode();
            result = 31 * result + _maxBsonObjectSize;
            return result;
        }

        private final Set<String> _names;
        private final Set<Tag> _tags;
        private final boolean _isMaster;
        private final boolean _isSecondary;
        private final String _setName;
    }
    
    
    @Immutable
    static final class ReplicaSetErrorStatus{
        final boolean ok;
        final String error;
        
        ReplicaSetErrorStatus(boolean ok, String error){
            this.ok = ok;
            this.error = error;
        }
        
        public boolean isOk(){
            return ok;
        }
        
        public String getError(){
            return error;
        }
    }

    // Simple class to hold a single tag, both key and value
    @Immutable
    static final class Tag {
        final String key;
        final String value;

        Tag(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tag tag = (Tag) o;

            if (key != null ? !key.equals(tag.key) : tag.key != null) return false;
            if (value != null ? !value.equals(tag.value) : tag.value != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
        
        public DBObject toDBObject(){
            return new BasicDBObject(key, value);
        }
    }

    // Represents the state of a node in the replica set.  Instances of this class are mutable.
    static class UpdatableReplicaSetNode extends UpdatableNode {

        UpdatableReplicaSetNode(ServerAddress addr,
                                List<UpdatableReplicaSetNode> all,
                                AtomicReference<Logger> logger,
                                Mongo mongo,
                                MongoOptions mongoOptions,
                                AtomicReference<String> lastPrimarySignal) {
            this(addr, null, all, logger, mongo, mongoOptions, lastPrimarySignal);
        }

        UpdatableReplicaSetNode(ServerAddress addr,
                                ServerAddress altAddr,
                                List<UpdatableReplicaSetNode> all,
                                AtomicReference<Logger> logger,
                                Mongo mongo,
                                MongoOptions mongoOptions,
                                AtomicReference<String> lastPrimarySignal) {
            super(addr, altAddr, mongo, mongoOptions);
            _all = all;
            _names.add(addr.toString());
            _logger = logger;
            _lastPrimarySignal = lastPrimarySignal;
        }

        private ServerAddress myMeAddr( CommandResult isMasterResult ) {
            try {
                if ( isMasterResult != null && isMasterResult.ok() )
                    return new ServerAddress( isMasterResult.getString( "me" ) );
            } catch (Exception e) {
                // meh.
            }
            return _addr;
        }

        private void updateAddr() {
            ServerAddress addy = getServerAddress();
            try {
                if ( addy.updateInetAddress() ) {
                    updateAddrForReal();
                }
            } catch (UnknownHostException ex) {
                _logger.get().log(Level.WARNING, null, ex);
            }
        }

        private void updateAddrForReal() {
            ServerAddress addy = getServerAddress();
            // address changed, we've been given to understand; need to use new ports
            _port = new DBPort( addy, null, _mongoOptions );
            _mongo.getConnector().updatePortPool( addy );
            _logger.get().log( Level.INFO, "Address of host " + getHostAddrDesc() + " changed to " +
                                           addy.getSocketAddress().toString() );
        }

        private void updateAltAddrMaybe( ServerAddress altAddy ) {
            if ( ! altAddy.equals( _altAddr ) ) {
                _altAddr = altAddy;
                updateAddrForReal();
            }
        }

        /**
         *
         * @param seenNodes starts empty, and accrues all the nodes that each node has seen as this method is called
         *                  on each node in turn.  however, each of those other nodes will have to wait until it is
         *                  itself visited before it can be annotated with its _altAddr tag (if any).
         *                  If seenNodes is nonNull, it is because self is a member of seenNodes already.
         */
        void update( Set<UpdatableReplicaSetNode> seenNodes,
                     CommandResult isMasterResult,
                     Map<ServerAddress,ServerAddress> altAddrProj )
        {
            if ( altAddrProj != null && altAddrProj.containsKey( _addr ) ) {
                this.updateAltAddrMaybe( altAddrProj.get( _addr ) ); // ==> a new _port just in time to use below...
            }
            if ( altAddrProj != null && altAddrProj.containsValue( _addr ) ) {
                ServerAddress nominalAddr = keyForValue( altAddrProj, _addr );
                UpdatableReplicaSetNode node = _addIfNotHere( nominalAddr.toString() );
                if ( node != null && seenNodes != null ) {
                    node.updateAltAddrMaybe( _addr );
                    seenNodes.add( node );
                }
            }

            CommandResult res = isMasterResult;
            if ( res == null ) {
                if ( seenNodes != null ) {
                    final String wow = "Newly discovered server!  Check it: " + getHostAddrDesc();
                    _logger.get().log( Level.INFO, wow );
                }
                res = update();
            }
            if (res == null || !_ok) {
                return;
            }

            _isMaster = res.getBoolean("ismaster", false);
            _isSecondary = res.getBoolean("secondary", false);
            _lastPrimarySignal.set(res.getString("primary"));  // N.B. - the *nominal* server name, as per rs.conf()

            // Tags were added in 2.0 but may not be present
            if (res.containsField("tags")) {
                DBObject tags = (DBObject) res.get("tags");
                for (String key : tags.keySet()) {
                    _tags.put(key, tags.get(key).toString());
                    if ( key.equals( alternateAddressTagName ) ) { // a hack by MongoLab.  you're welcome.  :)
                        String altAddrString = tags.get( key ).toString();
                        try {
                            updateAltAddrMaybe( new ServerAddress( altAddrString ) );
                        } catch ( UnknownHostException e ) {
                            String msg = "Unable to set alternate address [" + altAddrString + "]";
                            getLogger().log( Level.WARNING, msg, e );
                        }
                    }
                }
            }

            if (res.containsField("hosts")) {                // N.B. - all *nominal* server names, as per rs.conf()
                for (Object x : (List) res.get("hosts")) {
                    String host = x.toString();
                    UpdatableReplicaSetNode node = _addIfNotHere(host);
                    if (node != null && seenNodes != null)
                        seenNodes.add(node);
                }
            }

            if (res.containsField("passives")) {             // N.B. - all *nominal* server names, as per rs.conf()
                for (Object x : (List) res.get("passives")) {
                    String host = x.toString();
                    UpdatableReplicaSetNode node = _addIfNotHere(host);
                    if (node != null && seenNodes != null)
                        seenNodes.add(node);
                }
            }

            //old versions of mongod don't report setName
            if (res.containsField("setName")) {
                _setName = res.getString("setName", "");
                
                if(_logger.get() == null)
                    _logger.set(Logger.getLogger(_rootLogger.getName() + "." + _setName));
            }
        }

        @Override
        protected Logger getLogger() {
            return _logger.get();
        }

        /**
         * If it's not already known, creates and adds to _all a node based on
         * @param host -- a *nominal* host address string, as seen in rs.conf().
         * @return the node found or created for host
         */
        UpdatableReplicaSetNode _addIfNotHere(String host) {
            UpdatableReplicaSetNode n = findNode(host, _all, _logger);
            if (n == null) {
                try {
                    n = new UpdatableReplicaSetNode(new ServerAddress(host), _all, _logger, _mongo, _mongoOptions, _lastPrimarySignal);
                    _all.add(n);
                } catch (UnknownHostException un) {
                    _logger.get().log(Level.WARNING, "couldn't resolve host [" + host + "]");
                }
            }
            return n;
        }

        /**
         * find a node based on *nominal* host address string, as seen in rs.conf().
         */
        private UpdatableReplicaSetNode findNode(String host, List<UpdatableReplicaSetNode> members, AtomicReference<Logger> logger) {
            for (UpdatableReplicaSetNode node : members)
                if (node._names.contains(host))
                    return node;

            ServerAddress addr;
            try {
                addr = new ServerAddress(host);
            } catch (UnknownHostException un) {
                logger.get().log(Level.WARNING, "couldn't resolve host [" + host + "]");
                return null;
            }

            for (UpdatableReplicaSetNode node : members) {
                if (node._addr.equals(addr)) {
                    node._names.add(host);
                    return node;
                }
            }

            return null;
        }

        public void close() {
            _port.close();
            _port = null;
        }

        private final Set<String> _names = Collections.synchronizedSet(new HashSet<String>());
        final LinkedHashMap<String, String> _tags = new LinkedHashMap<String, String>();

        boolean _isMaster = false;
        boolean _isSecondary = false;
        String _setName;

        private final AtomicReference<Logger> _logger;
        private final AtomicReference<String> _lastPrimarySignal;
        private final List<UpdatableReplicaSetNode> _all;
    }

    // Thread that monitors the state of the replica set.  This thread is responsible for setting a new ReplicaSet
    // instance on ReplicaSetStatus.members every pass through the members of the set.
    class Updater extends BackgroundUpdater {

        Updater(List<ServerAddress> initial){
            super("ReplicaSetStatus:Updater");
            _all = new ArrayList<UpdatableReplicaSetNode>(initial.size());
            for ( ServerAddress addr : initial ){
                _all.add( new UpdatableReplicaSetNode( addr, _all,  _logger, _mongo, _mongoOptions, _lastPrimarySignal ) );
            }
            _nextResolveTime = System.currentTimeMillis() + inetAddrCacheMS;
        }

        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    int curUpdateIntervalMS = updaterIntervalNoMasterMS;

                    try {
                        updateAll();

                        updateInetAddresses();

                        ReplicaSet replicaSet = new ReplicaSet(createNodeList(), _random, slaveAcceptableLatencyMS);
                        _replicaSetHolder.set(replicaSet);

                        if (replicaSet.getErrorStatus().isOk() && replicaSet.hasMaster()) {
                            _mongo.getConnector().setMaster(replicaSet.getMaster());
                            curUpdateIntervalMS = updaterIntervalMS;
                        }
                    } catch (Exception e) {
                        _logger.get().log(Level.WARNING, "couldn't do update pass", e);
                    }

                    Thread.sleep(curUpdateIntervalMS);
                }
            }
            catch (InterruptedException e) {
               // Allow thread to exit
            }

            _replicaSetHolder.close();
            closeAllNodes();
        }

        public synchronized void updateAll(){
            HashSet<UpdatableReplicaSetNode> seenNodes = new HashSet<UpdatableReplicaSetNode>();
            Map<ServerAddress,CommandResult> allsIsMasterRunned = allRunIsMasterCmd();
            Map<ServerAddress,ServerAddress> altAddrProj = makeAltAddrProjection( allsIsMasterRunned );

            for ( int i=0; i<_all.size(); i++ ){
                UpdatableReplicaSetNode n = _all.get(i);
                // _all membership often changes during this loop, so ...
                CommandResult isMasterResult = ( allsIsMasterRunned.containsKey( n._addr ) ?
                                                 allsIsMasterRunned.get( n._addr ) :
                                                 null );
                n.update( seenNodes, isMasterResult, altAddrProj );
            }

            if (seenNodes.size() > 0) {
                // not empty, means that at least 1 server gave node list
                // remove unused hosts
                Iterator<UpdatableReplicaSetNode> it = _all.iterator();
                while (it.hasNext()) {
                    if (!seenNodes.contains(it.next()))
                        it.remove();
                }
            }
        }

        Map<ServerAddress,CommandResult> allRunIsMasterCmd() {
            Map<ServerAddress,CommandResult> result = new HashMap<ServerAddress, CommandResult>( _all.size() );
            for ( UpdatableNode n : _all ) {
                result.put( n._addr, n.update() );
            }
            return result;
        }

        synchronized Map<ServerAddress,ServerAddress> makeAltAddrProjection( Map<ServerAddress,CommandResult> isMasterResults) {
            Map<ServerAddress,ServerAddress> result = new HashMap<ServerAddress, ServerAddress>( _all.size() );
            for ( UpdatableReplicaSetNode n : _all ) {
                ServerAddress nominalAddr = n.myMeAddr( isMasterResults.get( n._addr ) );
                if ( ! nominalAddr.equals( n._addr ) ) {
                    // Houston, we have a projection!
                    result.put( nominalAddr, n._addr );
                }
            }
            return result.size() > 0 ? result : null ;
        }

        private List<ReplicaSetNode> createNodeList() {
            List<ReplicaSetNode> nodeList = new ArrayList<ReplicaSetNode>(_all.size());
            for (UpdatableReplicaSetNode cur : _all) {
                nodeList.add(new ReplicaSetNode(cur._addr, cur._altAddr, cur._names, cur._setName, cur._pingTimeMS,
                                                cur._ok, cur._isMaster, cur._isSecondary, cur._tags, cur._maxBsonObjectSize));
            }
            return nodeList;
        }

        private void updateInetAddresses() {
            long now = System.currentTimeMillis();
            if (inetAddrCacheMS > 0 && _nextResolveTime < now) {
                _nextResolveTime = now + inetAddrCacheMS;
                for (UpdatableReplicaSetNode node : _all) {
                    node.updateAddr();
                }
            }
        }

        private void closeAllNodes() {
            for (UpdatableReplicaSetNode node : _all) {
                try {
                    node.close();
                } catch (final Throwable t) { /* nada */ }
            }
        }

        private final List<UpdatableReplicaSetNode> _all;
        private volatile long _nextResolveTime;
        private final Random _random = new Random();
    }

    @Override
    Node ensureMaster() {
        ReplicaSetNode masterNode = getMasterNode();
        if (masterNode != null) {
            return masterNode;
        }

        _replicaSetHolder.waitForNextUpdate();

        masterNode = getMasterNode();
        if (masterNode != null) {
            return masterNode;
        }

        return null;
    }

    List<ServerAddress> getServerAddressList() {
        List<ServerAddress> addrs = new ArrayList<ServerAddress>();
        for (ReplicaSetNode node : _replicaSetHolder.get().getAll())
            addrs.add(node.getNominalServerAddress());
        return addrs;
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server.
     * Note that this value may change over time depending on which server is master.
     * @return the maximum size, or 0 if not obtained from servers yet.
     * @throws MongoException
     */
    public int getMaxBsonObjectSize() {
        return _replicaSetHolder.get().getMaxBsonObjectSize();
    }

    final ReplicaSetHolder _replicaSetHolder = new ReplicaSetHolder();

    // will get changed to use set name once its found
    private final AtomicReference<Logger> _logger = new AtomicReference<Logger>(_rootLogger);

    private final AtomicReference<String> _lastPrimarySignal = new AtomicReference<String>();
    final static int slaveAcceptableLatencyMS;
    final static int inetAddrCacheMS;
    final static String alternateAddressTagName;

    static {
        slaveAcceptableLatencyMS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        inetAddrCacheMS = Integer.parseInt( System.getProperty( "com.mongodb.inetAddrCacheMS", "300000" ) );
        alternateAddressTagName = System.getProperty( "com.mongodb.memberAltAddressTag", "altAddress" );
    }

    static <T> T keyForValue( Map<T,T> mappy, T val ) {
        for ( Map.Entry<T,T> entry : mappy.entrySet() ) {
            if ( entry.getValue().equals( val ) )
                return entry.getKey();
        }
        return null;
    }

}
