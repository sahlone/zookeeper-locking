package com.zycus.zookeeper.lock;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;

/**
 * @author sahil.lone
 *
 */
public class LockUtil
{
	private static final Logger	LOG	= Logger.getLogger(BlockingZookeeperLock.class);

	public static Lock lock(String lockName)
	{
		BlockingZookeeperLock blockingZookeeperLock = null;
		try
		{
			blockingZookeeperLock = new BlockingZookeeperLock(lockName);
			blockingZookeeperLock.lock();
		}
		catch (Exception e)
		{
			if (null != blockingZookeeperLock)
			{
				try
				{
					blockingZookeeperLock.unlock();
				}
				catch (RuntimeException e1)
				{
					LOG.warn("Caught: " + e, e);
				}
			}
			throw new RuntimeException(e);
		}
		return blockingZookeeperLock;
	}

	public static Lock lock(Set<String> keys)
	{
		String lockKey = join(keys);
		return lock(lockKey);
	}

	public static String join(Set<String> keys)
	{
		StringBuilder stringBuilder = new StringBuilder();
		for (String tempString : keys)
		{
			stringBuilder.append(tempString).append(LockSettings.LOCK_DESCRIMINATOR.stringValue());
		}

		return stringBuilder.substring(0, stringBuilder.length() - 1);
	}

	public static Lock lock(List<String> asList)
	{
		return lock(new HashSet<String>(asList));
	}

	public static void unlock(Lock lock)
	{
		BlockingZookeeperLock blockingZookeeperLock = null;
		try
		{
			if (lock instanceof BlockingZookeeperLock && lock != null)
			{
				lock.unlock();
			}
		}
		catch (Exception e)
		{
			if (null != blockingZookeeperLock)
			{
				blockingZookeeperLock.unlock();
			}

		}
	}

}
