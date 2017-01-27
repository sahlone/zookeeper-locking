package com.zycus.zookeeper.lock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * @author sahil.lone
 *
 */
public class ZookeeperLockReaper implements Runnable
{
	private static final Logger		LOG	= Logger.getLogger(BlockingZookeeperLock.class);

	private static CuratorFramework	zk	= null;

	public ZookeeperLockReaper(CuratorFramework zookeeper)
	{
		zk = zookeeper;
	}

	@Override
	public void run()
	{

		while (true)
		{
			try
			{
				long minTTL = LockSettings.LOCK_MAINTAIN_TIMEOUT.intValue();
				List<String> names = zk.getChildren().forPath(LockSettings.LOCK_BASE_PATH.stringValue());
				for (String name : names)
				{
					long ttl = isLockDormant(LockSettings.LOCK_BASE_PATH.stringValue() + "/" + name);

					if (ttl < minTTL)
					{
						minTTL = ttl;
					}

				}
				Thread.sleep(minTTL * 1000);
			}
			catch (Exception e)
			{
				LOG.error(
						"ERROR_LOCK_REAPER: Error while deleting dormant locks in Zookeeper. It maybe the node is already deleted. Please refer the exception for the rest",
						e);
			}
		}

	}

	public CuratorFramework getZookeeper()
	{
		return zk;
	}

	public void setZookeeper(CuratorFramework zookeeper)
	{
		zk = zookeeper;
	}

	public static long isLockDormant(String nodePath) throws Exception
	{

		Stat stat = zk.checkExists().forPath(nodePath);
		if (null != stat)
		{
			long ttl = LockSettings.LOCK_MAINTAIN_TIMEOUT.intValue()
					- ((System.currentTimeMillis() - stat.getMtime()) / 1000);
			if (ttl < 0)
			{
				try
				{
					if (Arrays.equals(LockSettings.LOCK_OWNER_IDENTIFICATION_BYTES.getBytesData(), zk.getData()
							.forPath(nodePath)))
					{
						zk.delete().forPath(nodePath);
					}
				}
				catch (Exception e)
				{
					if (!(e instanceof KeeperException.NoNodeException))
					{
						throw e;
					}
				}
			}
			else
			{
				return ttl;
			}

		}
		return LockSettings.LOCK_MAINTAIN_TIMEOUT.intValue();
	}

}
