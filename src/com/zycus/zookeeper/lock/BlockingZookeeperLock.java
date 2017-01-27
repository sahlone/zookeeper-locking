package com.zycus.zookeeper.lock;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * @author sahil.lone
 *
 */
public class BlockingZookeeperLock implements Lock
{
	private static final Logger		LOG					= Logger.getLogger(BlockingZookeeperLock.class);
	private String					lockName;
	private CountDownLatch			lockAcquiredSignal	= new CountDownLatch(1);
	private static CuratorFramework	zooKeeper			= null;

	private String					lockId;

	private ZNodeName				idName;
	private String					ownerId;
	private String					blockingLockId;
	private byte[]					data				= { 0x12, 0x34 };
	private long					retryDelay			= 500L;
	private int						retryCount			= 5;
	private String					RE_ENTRANT_ID		= null;
	private static boolean			isInitialized		= false;

	@SuppressWarnings("unused")
	private BlockingZookeeperLock()
	{

	}

	public BlockingZookeeperLock(String lockName)
	{
		this.lockName = lockName;
		RE_ENTRANT_ID = Thread.currentThread().getId() + "";
		if (!isInitialized)
		{

			if (null == LockSettings.ZK_CONNECTION_STRING.getValue()
					|| LockSettings.ZK_CONNECTION_STRING.stringValue().isEmpty())
			{
				throw new RuntimeException(
						"ZOOKEEPER_EMPTY_URL: Please populate the zookeeper connection string in LockSettings.class as the property value.");
			}
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			zooKeeper = CuratorFrameworkFactory.newClient(LockSettings.ZK_CONNECTION_STRING.stringValue(),
					LockSettings.SESSION_TIMEOUT.milliSecondsValue(),
					LockSettings.ZK_CONNECTION_TIMEOUT.milliSecondsValue(), retryPolicy);
			zooKeeper.start();

			ZookeeperLockReaper zookeeperLockReaper;
			zookeeperLockReaper = new ZookeeperLockReaper(zooKeeper);
			Thread lockReaper = new Thread(zookeeperLockReaper);
			lockReaper.setDaemon(true);
			lockReaper.start();

			isInitialized = true;
		}
	}

	private void lock(long timeout, TimeUnit unit) throws RuntimeException
	{
		LOG.debug(String.format("Requesting lock on %s at %s with timeout %d %s...\n", lockName,
				LockSettings.LOCK_BASE_PATH.stringValue(), timeout, unit.name()));
		ensureBaseLockPathExists(LockSettings.LOCK_BASE_PATH.stringValue(), null, LockSettings.DEFAULT_ACL.listValue(),
				CreateMode.PERSISTENT);
		boolean reentrant = checkForReentrant();
		if (reentrant)
		{
			return;
		}
		executeLockCreation();
		try
		{
			lockAcquiredSignal.await(timeout, unit);
		}
		catch (InterruptedException e)
		{
			throw new RuntimeException(e);
		}
		if (lockAcquiredSignal.getCount() == 0)
		{
			LOG.debug(String.format("Acquired lock %s on %s with timeout %d %s...\n", lockName,
					LockSettings.LOCK_BASE_PATH.stringValue(), timeout, unit.name()));
			return;
		}
		throw new RuntimeException(LockSettings.LOCK_ACQUIRE_EXCEPTION.stringValue());
	}

	private boolean checkForReentrant() throws RuntimeException
	{
		Boolean isReEntrant;
		Set<String> locksNeeded = new HashSet<String>(Arrays.asList(lockName.split(LockSettings.LOCK_DESCRIMINATOR
				.stringValue())));
		Set<String> locksRegistered = new HashSet<String>();
		List<String> names;
		try
		{
			names = zooKeeper.getChildren().forPath(LockSettings.LOCK_BASE_PATH.stringValue());
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		String prefix = "x" + LockSettings.LOCK_DESCRIMINATOR.stringValue() + RE_ENTRANT_ID
				+ LockSettings.LOCK_DESCRIMINATOR.stringValue();
		for (String name : names)
		{

			if (name.startsWith(prefix))
			{
				locksRegistered.addAll(Arrays.asList(name.split(LockSettings.LOCK_DESCRIMINATOR.stringValue())));
			}
		}
		if (locksRegistered.containsAll(locksNeeded))
		{
			isReEntrant = true;
		}
		else
		{
			locksNeeded.removeAll(locksRegistered);
			lockName = LockUtil.join(locksNeeded);
			isReEntrant = false;
		}
		return isReEntrant;
	}

	/**
	 * Attempts to acquire the exclusive write lock returning whether or not it was
	 * acquired. Note that the exclusive lock may be acquired some time later after
	 * this method has been invoked due to the current lock owner going away.
	 */
	@Override
	public synchronized void lock()
	{
		lock(LockSettings.LOCK_ACQUIRE_TIMEOUT.intValue(), TimeUnit.SECONDS);
	}

	/**
	 * Ensures that the given path exists with the given data, ACL and flags
	 * @param path
	 * @param acl
	 * @param flags
	 * @throws Exception 
	 */
	protected void ensureBaseLockPathExists(final String path, final byte[] data, final List<ACL> acl,
			final CreateMode flags) throws RuntimeException
	{
		Exception exception = null;
		for (int i = 0; i < retryCount; i++)
		{
			try
			{
				Stat stat = zooKeeper.checkExists().forPath(path);
				if (stat != null)
				{
					return;
				}
				zooKeeper.create().withMode(CreateMode.PERSISTENT).forPath(path, data);
			}
			catch (Exception e)
			{

				exception = e;
				LOG.debug("Attempt " + i + " failed with connection loss so " + "attempting to reconnect: " + e, e);
				retryDelay(i);
			}
		}
		throw new RuntimeException(exception);

	}

	/**
	 * Removes the lock or associated znode if 
	 * you no longer require the lock. this also 
	 * removes your request in the queue for locking
	 * in case you do not already hold the lock.
	 * @throws RuntimeException throws a runtime exception
	 * if it cannot connect to zookeeper.
	 */
	@Override
	public synchronized void unlock() throws RuntimeException
	{
		LOG.debug(String.format("Attempting Lock release  by %s on %s\n", lockName,
				LockSettings.LOCK_BASE_PATH.stringValue()));
		if (lockId != null)
		{
			// we don't need to retry this operation in the case of failure
			// as ZK will remove ephemeral files and we don't wanna hang
			// this process when closing if we cannot reconnect to ZK
			try
			{
				try
				{
					zooKeeper.delete().forPath(lockId);
				}
				catch (Exception e)
				{
					//this is there as it might happen that the node is already deleted because of timeout settings
					if (!(e instanceof KeeperException.NoNodeException))
					{
						throw e;
					}
				}
			}
			catch (InterruptedException e)
			{
				LOG.warn("Caught: " + e, e);
			}
			catch (Exception e)
			{
				LOG.warn("Caught: " + e, e);
				throw new RuntimeException(e.getMessage(), e);
			}
			LOG.debug(String.format("Lock %s released  on %s ...\n", lockName,
					LockSettings.LOCK_BASE_PATH.stringValue()));
		}
	}

	/**
	 * the command that is run and retried for actually 
	 * obtaining the lock
	 * @return if the command was successful or not
	 */
	public void executeLockCreation() throws RuntimeException
	{
		Set<String> locksNeeded = new HashSet<String>(Arrays.asList(lockName.split(LockSettings.LOCK_DESCRIMINATOR
				.stringValue())));
		try
		{
			int creationRetries = 0;
			do
			{
				if (lockId == null)
				{
					String prefix = "x" + LockSettings.LOCK_DESCRIMINATOR.stringValue() + RE_ENTRANT_ID
							+ LockSettings.LOCK_DESCRIMINATOR.stringValue() + lockName
							+ LockSettings.LOCK_DESCRIMINATOR.stringValue();
					String lockPath = LockSettings.LOCK_BASE_PATH.stringValue() + "/" + prefix;

					List<String> names = zooKeeper.getChildren().forPath(LockSettings.LOCK_BASE_PATH.stringValue());
					for (String name : names)
					{
						if (name.startsWith(prefix))
						{
							lockId = name;
							LOG.debug("Found id created last time: " + lockId);
							break;
						}
					}
					if (lockId == null)
					{
						lockId = zooKeeper.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
								.withACL(LockSettings.DEFAULT_ACL.listValue()).forPath(lockPath, data);

						LOG.debug("Created id: " + lockId);
						creationRetries++;
					}

					idName = new ZNodeName(lockId);
				}

				if (lockId != null)
				{
					List<String> names = zooKeeper.getChildren().forPath(LockSettings.LOCK_BASE_PATH.stringValue());
					if (zooKeeper.checkExists().forPath(lockId) == null)
					{
						LOG.warn("No children in: " + LockSettings.LOCK_BASE_PATH.stringValue() + " when we've just "
								+ "created one! Lets recreate it...");
						lockId = null;
						if (creationRetries > retryCount)
						{
							throw new RuntimeException(
									"LOCK_CREATION_FAILED: Not able to create lock in zookeeper after max retries.");
						}
					}
					else
					{
						SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
						for (String name : names)
						{
							Set<String> locksRegistered = new HashSet<String>(Arrays.asList(name
									.split(LockSettings.LOCK_DESCRIMINATOR.stringValue())));

							if (!Collections.disjoint(locksRegistered, locksNeeded))
							{
								sortedNames.add(new ZNodeName(LockSettings.LOCK_BASE_PATH.stringValue() + "/" + name));
							}
						}
						ownerId = sortedNames.first().getName();
						ZookeeperLockReaper.isLockDormant(ownerId);
						SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(idName);
						if (!lessThanMe.isEmpty())
						{
							ZNodeName lastChildName = lessThanMe.last();
							blockingLockId = lastChildName.getName();
							if (LOG.isDebugEnabled())
							{
								LOG.debug("Watching less than me nodes: " + blockingLockId);
							}
							Stat stat = zooKeeper.checkExists().usingWatcher(new LockWatcher()).forPath(blockingLockId);
							if (stat == null)
							{
								executeLockCreation();
							}
						}
						else
						{
							if (isOwner())
							{

								lockAcquired();
							}
						}
					}
				}
			}
			while (lockId == null);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public String getLockName()
	{
		return lockName;
	}

	public void setLockName(String lockName)
	{
		this.lockName = lockName;
	}

	/**
	 * Returns true if this node is the owner of the
	 *  lock (or the leader)
	 */
	public boolean isOwner()
	{
		return lockId != null && ownerId != null && lockId.equals(ownerId);
	}

	/**
	 * return the id for this lock
	 * @return the id for this lock
	 */
	public String getId()
	{
		return this.lockId;
	}

	protected void retryDelay(int attemptCount)
	{
		if (attemptCount > 0)
		{
			try
			{
				Thread.sleep(attemptCount * retryDelay);
			}
			catch (InterruptedException e)
			{
				LOG.debug("Failed to sleep: " + e, e);
			}
		}
	}

	public void lockAcquired() throws Exception
	{
		zooKeeper.setData().forPath(ownerId, LockSettings.LOCK_OWNER_IDENTIFICATION_BYTES.getBytesData());
		LOG.debug(String.format("Lock acquired by %s on %s\n", lockName, LockSettings.LOCK_BASE_PATH.stringValue()));
		lockAcquiredSignal.countDown();
	}

	private class LockWatcher implements CuratorWatcher
	{
		public void process(WatchedEvent event)
		{
			// lets either become the leader or watch the new/updated node
			LOG.debug("Watcher fired on path: " + event.getPath() + " state: " + event.getState() + " type "
					+ event.getType());
			try
			{
				executeLockCreation();
			}
			catch (Exception e)
			{
				LOG.warn("Failed to acquire lock: " + e, e);
			}
		}
	}

	@Override
	protected Object clone() throws CloneNotSupportedException
	{
		throw new CloneNotSupportedException();
	}

	public CuratorFramework getZooKeeper()
	{
		return zooKeeper;
	}

	public void setZooKeeper(CuratorFramework zooKeeper)
	{
		this.zooKeeper = zooKeeper;
	}

	@Override
	public void lockInterruptibly() throws InterruptedException
	{

	}

	@Override
	public boolean tryLock()
	{
		try
		{
			return tryLock(1, TimeUnit.SECONDS);
		}
		catch (InterruptedException e)
		{
			return false;
		}
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
	{
		try
		{
			lock(time, unit);
			return true;
		}
		catch (Exception e)
		{
			return false;
		}

	}

	@Override
	public Condition newCondition()
	{
		return null;
	}

	public String getLockId()
	{
		return lockId;
	}

}
