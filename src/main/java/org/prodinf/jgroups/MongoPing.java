package org.prodinf.jgroups;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.PingData;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.WriteConcern;

/**
 * The mongo discovery protocol.
 * 
 * @author acostin
 */
public class MongoPing extends Discovery {
	private static final String EMPTY_STR = "";

	private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(MongoPing.class);

	@Property(description = "mongo url connection")
	public String mongourl = "mongodb://localhost:27017";
	@Property(description = "Interval (in milliseconds) at which the own Address is written. 0 disables it.")
	public long interval = 30000;

	@Property(description = "name of the ping column in the mongo collection")
	public String pingColumnName = "PING_DATA";

	@Property(description = "name of the cluster column in the mongo collection")
	public String clusterColumnName = "cluster";

	@Property(description = "name of the address column in the mongo collection")
	public String addresColumnName = "ADDRES";

	@Property(description = "name of the database where the members are stored")
	public String databaseName = "COMMUNICATION";

	@Property(description = "name of the collection from the database where the members are stored")
	public String collectionName = "COMMUNICATION";

	private DBCollection mongoCollection = null;
	private Mongo mongo;

	// private int random = -1;

	public MongoPing() {
		if (log.isInfoEnabled()) {
			log.info("MongoPing protocol created instance");
		}
	}

	@Override
	public void init() throws Exception {
		super.init();

		closeMongoInstance();

	}

	private void initMongoConnection() throws UnknownHostException {
		closeMongoInstance();
		mongo = new com.mongodb.Mongo(new MongoURI(this.mongourl));
		mongoCollection = mongo.getDB(this.databaseName).getCollection(this.collectionName);
		mongoCollection.ensureIndex(new BasicDBObject(this.clusterColumnName, Integer.valueOf(1)));
		mongoCollection.ensureIndex(new BasicDBObject(this.addresColumnName, Integer.valueOf(1)));
	}

	private void closeMongoInstance() {
		try {
			if (mongo != null) {
				mongo.close();
				mongo = null;
			}
		} catch (Exception e) {
			log.error("Error closing  previous mongo connection. Execution will continue ", e);
		}
	}

	private Future<?> writer_future;

	@Override
	public void start() throws Exception {
		super.start();
		initMongoConnection();
		if (interval > 0) {
			if (log.isInfoEnabled()) {
				log.info("Started mongo timer");
			}
			writer_future = timer.scheduleWithFixedDelay(new WriteMongo(), interval, interval, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void stop() {
		if (writer_future != null) {
			writer_future.cancel(false);
			writer_future = null;
		}
		remove(name, local_addr, WriteConcern.SAFE);
		closeMongoInstance();
		super.stop();
	}

	@Override
	public void destroy() {
		if (writer_future != null) {
			writer_future.cancel(false);
			writer_future = null;
		}
		closeMongoInstance();
		remove(name, local_addr, WriteConcern.NONE);
		super.destroy(); // To change body of generated methods, choose Tools |
		                 // Templates.
	}

	@Override
	public Object down(Event evt) {
		Object retval = super.down(evt);
		if (evt.getType() == Event.VIEW_CHANGE) {
			handleView((View) evt.getArg());
		}
		return retval;
	}

	protected void handleView(View view) {
		Collection<Address> mbrs = view.getMembers();
		boolean is_coordinator = !mbrs.isEmpty() && mbrs.iterator().next().equals(local_addr);
		if (is_coordinator) {
			List<PingData> data = readAll(group_addr);
			int removedAddreses = 0;
			for (PingData entry : data) {
				Address addr = entry.getAddress();
				if (addr != null && !mbrs.contains(addr)) {
					remove(group_addr, addr, WriteConcern.SAFE);
					removedAddreses++;
				}
			}
			if (removedAddreses > 0) {
				if (log.isInfoEnabled()) {
					log.info("Removed " + removedAddreses + " adresses that are old");
				}
			}

		}
	}

	protected void remove(String clustername, Address addr, WriteConcern concern) {
		if (clustername == null || addr == null) {
			return;
		}
		String uk = addressAsString(addr);

		if (log.isInfoEnabled()) {
			log.info("removing address " + uk + " from cluster" + clustername);
		}
		mongoCollection.remove(new BasicDBObject(this.addresColumnName, uk), concern);

	}

	private synchronized PingData readValue(DBObject memeber) {
		PingData retval = null;
		DataInputStream in = null;

		try {
			in = new DataInputStream(new ByteArrayInputStream((byte[]) memeber.get(pingColumnName)));
			PingData tmp = new PingData();
			tmp.readFrom(in);
			return tmp;
		} catch (Exception e) {
			log.debug("failed to read file : ", e);
		} finally {
			Util.close(in);
		}
		return retval;
	}

	/**
	 * Reads all information from the given directory under clustername
	 * 
	 * @return
	 */
	protected synchronized List<PingData> readAll(String clustername) {
		if (log.isDebugEnabled()) {
			log.debug("reading all : " + clustername);
		}
		BasicDBObject bObject = new BasicDBObject(this.clusterColumnName, clustername);
		List<DBObject> mongoMembers = this.mongoCollection.find(bObject).toArray();
		if (log.isInfoEnabled()) {
			log.info("reading all : " + mongoMembers);
		}
		List<PingData> retval = new ArrayList<PingData>(mongoMembers.size());
		for (DBObject member : mongoMembers) {
			PingData data = readValue(member);
			if (data == null) {
				// remove all bad members
				this.mongoCollection.remove(member);
			}
			retval.add(data);
		}

		return retval;
	}

	protected class WriteMongo implements Runnable {

		@Override
		public void run() {
			try {
				PhysicalAddress physical_addr = (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
				List<PhysicalAddress> physical_addrs = Arrays.asList(physical_addr);
				PingData data = new PingData(local_addr, null, false, UUID.get(local_addr), physical_addrs);
				writeToMongo(data, group_addr);
				if (log.isInfoEnabled()) {
					log.info("Done writing to mongo");
				}
			} catch (Exception ex) {
				log.error("Unable to serialize Ping Data in mongo", ex);
			}
		}
	}

	protected String addressAsString(Address address) {
		if (address == null) {
			return EMPTY_STR;
		}
		if (address instanceof UUID) {
			return ((UUID) address).toStringLong();
		}
		return address.toString();
	}

	private synchronized void writeToMongo(PingData data, String clustername) {

		try {
			if (data == null) {
				return;
			}
			// asta va fi UK
			String filename = addressAsString(local_addr);
			BasicDBObject row = new BasicDBObject(this.addresColumnName, filename);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			data.writeTo(new DataOutputStream(baos));
			byte[] blob = baos.toByteArray();

			row.put(pingColumnName, blob);
			row.put(this.clusterColumnName, clustername);
			// we tried to do upsert in mongo
			if (log.isInfoEnabled()) {
				log.info("Saving rowing mongo :" + row);
			}
			this.mongoCollection.update(new BasicDBObject(this.addresColumnName, filename), row, true, false, WriteConcern.SAFE);
		} catch (Exception exception) {
			log.error("Error writing to mongo");
		}
	}

	@Override
	public Collection<PhysicalAddress> fetchClusterMembers(String cluster_name) {
		List<PingData> existing_mbrs = readAll(cluster_name);

		PhysicalAddress physical_addr = (PhysicalAddress) down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
		List<PhysicalAddress> physical_addrs = Arrays.asList(physical_addr);
		PingData data = new PingData(local_addr, null, false, UUID.get(local_addr), physical_addrs);
		writeToMongo(data, cluster_name); // write my own data to file

		// If we don't find any files, return immediately
		if (existing_mbrs.isEmpty()) {
			return Collections.emptyList();
		}
		Set<PhysicalAddress> retval = new HashSet<PhysicalAddress>(existing_mbrs.size());

		for (PingData tmp : existing_mbrs) {
			Collection<PhysicalAddress> dests = tmp != null ? tmp.getPhysicalAddrs() : null;
			if (dests == null) {
				continue;
			}
			for (final PhysicalAddress dest : dests) {
				if (dest == null) {
					continue;
				}
				retval.add(dest);
			}
		}

		return retval;
	}

	@Override
	public boolean sendDiscoveryRequestsInParallel() {
		return true;
	}

	@Override
	public boolean isDynamic() {
		return true;
	}
}
