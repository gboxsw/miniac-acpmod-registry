package com.gboxsw.miniac.gateways.acpregistry;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.gboxsw.acpmod.registry.AutoUpdater;
import com.gboxsw.acpmod.registry.Register;
import com.gboxsw.acpmod.registry.RegisterCollection;
import com.gboxsw.acpmod.registry.RequestStatistics;
import com.gboxsw.acpmod.registry.XmlLoader;
import com.gboxsw.acpmod.registry.XmlLoader.RegisterCollectionConfig;
import com.gboxsw.miniac.*;
import com.gboxsw.miniac.dataitems.AliasDataItem;

/**
 * Gateway encapsulating a registry gateway.
 */
public class RegistryGateway extends Gateway {

	/**
	 * Default (pre-defined) name of the gateway.
	 */
	public static final String DEFAULT_ID = "acpreg";

	/**
	 * Extended record about an active register collection.
	 */
	private static class ActiveCollection {
		/**
		 * Register collection.
		 */
		final RegisterCollection collection;

		/**
		 * Latest statistical data of the collection.
		 */
		RequestStatistics latestStatistics;

		/**
		 * Constructs record of an active collection.
		 * 
		 * @param collection
		 *            the collection.
		 */
		ActiveCollection(RegisterCollection collection) {
			this.collection = collection;
		}
	}

	/**
	 * Default interval in seconds between two updates of statistical data.
	 */
	private static final int DEFAULT_STATISTICS_UPDATE_INTERVAL = 5;

	/**
	 * Updater of registers (singleton).
	 */
	private static final AutoUpdater commonRegistryUpdater = new AutoUpdater();

	/**
	 * Updater of registers.
	 */
	private final AutoUpdater registryUpdater;

	/**
	 * Registry gateway wrapped by this miniac gateway.
	 */
	private final com.gboxsw.acpmod.registry.Gateway gateway;

	/**
	 * Set of active registers, i.e., registers with data item in application.
	 */
	private final Set<Register> activeRegisters = new HashSet<>();

	/**
	 * Map of active collection.
	 */
	private final Map<String, ActiveCollection> activeCollections = new HashMap<>();

	/**
	 * Update interval in seconds between two updates of statistics.
	 */
	private int statisticsUpdateInterval = DEFAULT_STATISTICS_UPDATE_INTERVAL;

	/**
	 * Cancellable publisher of statistical data.
	 */
	private Cancellable statisticsUpdateGenerator;

	/**
	 * Register collections accessible by the gateway.
	 */
	private Map<String, RegisterCollectionConfig> collections = new HashMap<>();

	/**
	 * Mapping of register names to registers.
	 */
	private Map<String, Register> registersByName;

	/**
	 * Constructs gateway from configuration in an xml file using the default
	 * xml loader.
	 * 
	 * @param xmlFile
	 *            the xml file with configuration of gateway.
	 */
	public RegistryGateway(File xmlFile) {
		this(xmlFile, new XmlLoader());
	}

	/**
	 * Constructs gateway from configuration in an xml file and with common
	 * registry updater.
	 * 
	 * @param xmlFile
	 *            the xml file with configuration of gateway.
	 * @param xmlLoader
	 *            the configured loader used to load the xml file.
	 */
	public RegistryGateway(File xmlFile, XmlLoader xmlLoader) {
		this(xmlFile, xmlLoader, null);
	}

	/**
	 * Constructs gateway from configuration in an xml file.
	 * 
	 * @param xmlFile
	 *            the xml file with configuration of gateway.
	 * @param xmlLoader
	 *            the configured loader used to load the xml file.
	 * @param registryUpdater
	 *            the automatic updater of registers. If set to null, a registry
	 *            updater which is common for all gateways is used.
	 */
	public RegistryGateway(File xmlFile, XmlLoader xmlLoader, AutoUpdater registryUpdater) {
		this.registryUpdater = (registryUpdater == null) ? commonRegistryUpdater : registryUpdater;
		ArrayList<Register> registers = new ArrayList<>();

		gateway = xmlLoader.loadGatewayFromXml(xmlFile, collections, registers);

		registersByName = new HashMap<>();
		for (Register register : registers) {
			if (registersByName.containsKey(register.getName())) {
				throw new RuntimeException("Duplicated name of register: " + register.getName());
			}

			registersByName.put(register.getName(), register);
		}
	}

	/**
	 * Returns the update interval between two updates of statistical data.
	 * 
	 * @return the update interval in seconds or zero in the case when
	 *         statistical data are not published.
	 */
	public int getStatisticsUpdateInterval() {
		synchronized (getLock()) {
			return statisticsUpdateInterval;
		}
	}

	/**
	 * Sets the update interval between two updates of statistical data.
	 * 
	 * @param statisticsUpdateInterval
	 *            the update interval in seconds. The negative value or zero
	 *            disables publication of statistical data.
	 */
	public void setStatisticsUpdateInterval(int statisticsUpdateInterval) {
		synchronized (getLock()) {
			if (isRunning()) {
				throw new IllegalStateException(
						"It is not possible to change update interval when the gateway is running.");
			}

			statisticsUpdateInterval = Math.max(statisticsUpdateInterval, 0);
			this.statisticsUpdateInterval = statisticsUpdateInterval;
		}
	}

	/**
	 * Returns list of names of available registers. After the gateway is
	 * started, no registers are available.
	 * 
	 * @return the list of names.
	 */
	public List<String> getAvailableRegisters() {
		synchronized (getLock()) {
			if (isRunning() || (registersByName == null)) {
				return Collections.emptyList();
			} else {
				return new ArrayList<>(registersByName.keySet());
			}
		}
	}

	/**
	 * Adds a register based data item to the application to which the gateway
	 * is attached. The added data item is bound to register with given name.
	 * Each register can be bound only to one data item. If more data items
	 * bound to the same register are required, use {@link AliasDataItem}.
	 * 
	 * @param registerName
	 *            the name of register.
	 * @param gatewayId
	 *            the identifier of a gateway which will manage the data item.
	 *            The gateway must be instance of the class {@link DataGateway
	 *            DataGateway}.
	 * @param dataItemId
	 *            the identifier of data item within the gateway.
	 * @return the register data item added to the application.
	 */
	public RegisterDataItem<?> addRegisterToApplication(String registerName, String gatewayId, String dataItemId) {
		synchronized (getLock()) {
			if (!isAttachedToApplication()) {
				throw new IllegalStateException("The gateway is not attached to application.");
			}

			if (isRunning()) {
				throw new IllegalStateException("It is not possible to add data item to launched application.");
			}

			Register register = registersByName.get(registerName);
			if (register == null) {
				throw new IllegalArgumentException("Register with name \"" + registerName + "\" does not exist.");
			}

			if (activeRegisters.contains(register)) {
				throw new IllegalStateException(
						"Register with name \"" + registerName + "\" is already added to application.");
			}

			RegisterDataItem<?> registerDataItem = new RegisterDataItem<>(register, register.getType());
			getApplication().addDataItem(gatewayId, dataItemId, registerDataItem);
			activeRegisters.add(register);

			return registerDataItem;
		}
	}

	/**
	 * Adds a register based data item to the application to which the gateway
	 * is attached. The added data item is bound to register with given name.
	 * Each register can be bound only to one data item. If more data items
	 * bound to the same register are required, use {@link AliasDataItem}.
	 * 
	 * @param registerName
	 *            the name of register.
	 * @param gatewayId
	 *            the identifier of a gateway which will manage the data item.
	 *            The gateway must be instance of the class {@link DataGateway
	 *            DataGateway}.
	 * @param dataItemId
	 *            the identifier of data item within the gateway.
	 * @param type
	 *            the class of value type.
	 * @param <T>
	 *            the type of value.
	 * @return the register data item added to the application.
	 */
	@SuppressWarnings("unchecked")
	public <T> RegisterDataItem<T> addRegisterToApplication(String registerName, String gatewayId, String dataItemId,
			Class<T> type) {
		if (type == null) {
			throw new NullPointerException("Type of data item cannot be null.");
		}

		synchronized (getLock()) {
			if (!isAttachedToApplication()) {
				throw new IllegalStateException("The gateway is not attached to application.");
			}

			if (isRunning()) {
				throw new IllegalStateException("It is not possible to add data item to launched application.");
			}

			Register register = registersByName.get(registerName);
			if (register == null) {
				throw new IllegalArgumentException("Register with name \"" + registerName + "\" does not exist.");
			}

			if (activeRegisters.contains(register)) {
				throw new IllegalStateException(
						"Register with name \"" + registerName + "\" is already added to application.");
			}

			RegisterDataItem<?> registerDataItem = new RegisterDataItem<>(register, register.getType());
			getApplication().addDataItem(gatewayId, dataItemId, registerDataItem);
			activeRegisters.add(register);

			if (!register.getType().equals(type)) {
				throw new IllegalArgumentException("Type of register \"" + registerName + "\" is "
						+ register.getType().getName() + " and it does not match required type " + type.getName());
			}

			return (RegisterDataItem<T>) registerDataItem;
		}
	}

	@Override
	protected void onStart(Map<String, Bundle> bundles) {
		if (activeRegisters.isEmpty()) {
			return;
		}

		// find used register collections
		Set<RegisterCollection> usedCollections = new HashSet<>();
		for (Register register : activeRegisters) {
			usedCollections.add(register.getRegisterCollection());
		}

		// create map with named register collections
		for (Map.Entry<String, RegisterCollectionConfig> entry : collections.entrySet()) {
			if (usedCollections.contains(entry.getValue().registerCollection)) {
				activeCollections.put(entry.getKey(), new ActiveCollection(entry.getValue().registerCollection));
			}
		}

		// start underlying gateway
		gateway.start();

		// set registry hints for active collections
		for (Map.Entry<String, RegisterCollectionConfig> entry : collections.entrySet()) {
			RegisterCollectionConfig config = entry.getValue();
			if (usedCollections.contains(config.registerCollection)) {
				if (config.hintSettings != null) {
					registryUpdater.useRegistryHints(config.registerCollection, config.hintSettings);
				}
			}
		}

		// add registers
		registryUpdater.addRegisters(new ArrayList<Register>(activeRegisters));

		// release resources
		this.collections = Collections.emptyMap();
		this.registersByName = Collections.emptyMap();

		// prepare publication of statistical data (if enabled)
		if (statisticsUpdateInterval > 0) {
			statisticsUpdateGenerator = getApplication().invokeWithFixedDelay(new Runnable() {
				@Override
				public void run() {
					publishStatisticalData();
				}
			}, 0, statisticsUpdateInterval, TimeUnit.SECONDS);
		}
	}

	@Override
	protected void onAddTopicFilter(String topicFilter) {
		// nothing to do
	}

	@Override
	protected void onRemoveTopicFilter(String topicFilter) {
		// nothing to do
	}

	@Override
	protected void onPublish(Message message) {
		// nothing to do
	}

	@Override
	protected void onSaveState(Map<String, Bundle> outBundles) {
		// nothing to do
	}

	@Override
	protected void onStop() {
		if (activeRegisters.isEmpty()) {
			return;
		}

		// disable registry hints for all collections of registers
		Set<RegisterCollection> collections = new HashSet<>();
		for (Register register : activeRegisters) {
			collections.add(register.getRegisterCollection());
		}
		for (RegisterCollection collection : collections) {
			registryUpdater.disableRegistryHints(collection);
		}

		// remove all active registers from updater
		registryUpdater.removeRegisters(new ArrayList<Register>(activeRegisters));

		// stop underlying gateway
		gateway.stop();

		// release objects used to produce statistical data
		if (statisticsUpdateGenerator != null) {
			statisticsUpdateGenerator.cancel();
			statisticsUpdateGenerator = null;
		}
	}

	@Override
	protected boolean isValidTopicName(String topicName) {
		// no publications to gateway are allowed
		return false;
	}

	/**
	 * Publishes messages with statistical data.
	 */
	private void publishStatisticalData() {
		for (Map.Entry<String, ActiveCollection> entry : activeCollections.entrySet()) {
			String collectionName = entry.getKey();
			ActiveCollection activeCollection = entry.getValue();

			RequestStatistics statistics = activeCollection.collection.getStatistics().createSnapshot();
			handleReceivedMessage(new Message(collectionName + "/total", Long.toString(statistics.getTotalRequests())));
			handleReceivedMessage(
					new Message(collectionName + "/failed", Long.toString(statistics.getFailedRequests())));

			// compute current quality of communication link
			int quality = 100;
			if (activeCollection.latestStatistics != null) {
				long messagesInPeriod = statistics.getTotalRequests()
						- activeCollection.latestStatistics.getTotalRequests();
				long failsInPeriod = statistics.getFailedRequests()
						- activeCollection.latestStatistics.getFailedRequests();

				if (messagesInPeriod > 0) {
					quality = (int) Math
							.round((Math.max(messagesInPeriod - failsInPeriod, 0) / (double) messagesInPeriod) * 100);
				}
			}
			handleReceivedMessage(new Message(collectionName + "/quality", Integer.toString(quality)));

			activeCollection.latestStatistics = statistics;
		}
	}
}
