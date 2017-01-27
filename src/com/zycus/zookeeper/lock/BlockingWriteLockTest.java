package com.zycus.zookeeper.lock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author sahil.lone
 *
 */

public class BlockingWriteLockTest implements Callable<Void>
{

	private static CountDownLatch	cdl					= new CountDownLatch(1);
	private static long				lockReleasedTime	= -1;

	@BeforeClass
	public static void initLogger()
	{
		Logger logger = Logger.getLogger(BlockingZookeeperLock.class);

		ConsoleAppender consoleAppender = new ConsoleAppender();
		consoleAppender.activateOptions();
		consoleAppender.setLayout(new PatternLayout("[%d] %p %t - %m%n"));
		logger.setLevel(Level.ALL);
		//logger.setAdditivity(true);
		logger.addAppender(consoleAppender);
	}

	private static CuratorFramework	zooKeeper;

	@Before
	public void initClient()
	{

	}

	@Before
	public void tearDownBefore() throws Exception
	{
		if (null == zooKeeper)
		{
			LockSettings.ZK_CONNECTION_STRING.setValue("192.168.5.70:8000");
			if (null == LockSettings.ZK_CONNECTION_STRING.getValue()
					|| LockSettings.ZK_CONNECTION_STRING.stringValue().isEmpty())
			{
				throw new RuntimeException(
						"Please populate the zookeeper connection string in LockSettings.class as the property value.");
			}
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			BlockingWriteLockTest.zooKeeper = CuratorFrameworkFactory.newClient(
					LockSettings.ZK_CONNECTION_STRING.stringValue(), LockSettings.SESSION_TIMEOUT.milliSecondsValue(),
					LockSettings.ZK_CONNECTION_TIMEOUT.milliSecondsValue(), retryPolicy);
			zooKeeper.start();
		}
		List<String> children = zooKeeper.getChildren().forPath(LockSettings.LOCK_BASE_PATH.stringValue());
		for (String child : children)
		{
			try
			{
				zooKeeper.delete().forPath(LockSettings.LOCK_BASE_PATH.stringValue() + "/" + child);
			}
			catch (Exception e)
			{}
		}
	}

	@After
	public void tearDownAfter() throws Exception
	{
		if (null == zooKeeper)
		{
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			BlockingWriteLockTest.zooKeeper = CuratorFrameworkFactory.newClient(
					LockSettings.ZK_CONNECTION_STRING.stringValue(), LockSettings.SESSION_TIMEOUT.milliSecondsValue(),
					LockSettings.ZK_CONNECTION_TIMEOUT.milliSecondsValue(), retryPolicy);
			zooKeeper.start();
		}
		List<String> children = zooKeeper.getChildren().forPath(LockSettings.LOCK_BASE_PATH.stringValue());
		for (String child : children)
		{
			try
			{
				zooKeeper.delete().forPath(LockSettings.LOCK_BASE_PATH.stringValue() + "/" + child);
			}
			catch (Exception e)
			{}
		}
	}

	@Test
	public void testLock() throws Exception
	{
		Lock lock = LockUtil.lock("LockName1");
		assertNumberOfChildren(1);
		lock.unlock();
	}

	@Test
	public void reentrantLock() throws Exception
	{
		Lock lock = LockUtil.lock("LockName1");
		assertNumberOfChildren(1);
		Lock lock2 = LockUtil.lock("LockName1");
		assertNumberOfChildren(1);
		lock2.unlock();
		assertNumberOfChildren(1);
		lock.unlock();
		assertNumberOfChildren(0);
	}

	@Test
	public void testSetLock() throws Exception
	{
		String[] set = { "l1", "l2", "L3", "l1", "l2", "L3", "l1", "l2", "L3", "l1", "l2", "L3", "l1", "l2", "L3",
				"l1", "l2", "L3", "l1", "l2", "L3", "l1", "l2", "L3", "l1", "l2", "L3", "l1", "l2", "L3", "l1", "l2",
				"L3", "l1", "l2", "L3", "l1", "l2", "L3" };
		Lock lock = LockUtil.lock(Arrays.asList(set));
		Lock lock1 = LockUtil.lock(Arrays.asList(set));
		assertNumberOfChildren(1);

	}

	@Test
	public void blockingLock() throws Exception
	{
		Lock lock = LockUtil.lock("LockName1");
		assertNumberOfChildren(1);
		Lock lock2 = LockUtil.lock("LockName1");
		assertNumberOfChildren(1);
		lock2.unlock();
		assertNumberOfChildren(1);
		lock.unlock();
		assertNumberOfChildren(0);
	}

	@Test
	public void t1()
	{

		Set<String> lockKeys1 = new HashSet<String>();
		lockKeys1.add("lock1");
		lockKeys1.add("lock2");
		lockKeys1.add("lock3");
		lockKeys1.add("lock4");
		lockKeys1.add("lock5");
		lockKeys1.add("lock6");

		Set<String> lockKeys2 = new HashSet<String>();
		lockKeys2.add("lock1");
		lockKeys2.add("lock2");
		lockKeys2.add("lock3");

		Set<String> lockKeys3 = new HashSet<String>();
		lockKeys3.add("lock3");
		lockKeys3.add("lock4");
		lockKeys3.add("lock5");
		lockKeys3.add("lock9");

		Lock lock1 = LockUtil.lock(lockKeys1);
		Lock lock2 = LockUtil.lock(lockKeys2);
		Lock lock3 = LockUtil.lock(lockKeys3);
		lock1.unlock();
		LockUtil.unlock(lock2);
		LockUtil.unlock(lock3);

	}

	@Test
	public void concurrency()
	{
		ExecutorService executorService = Executors.newFixedThreadPool(100);

		List<Callable<Void>> taskList = new ArrayList<Callable<Void>>();
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());
		taskList.add(new BlockingWriteLockTest());

		try
		{

			System.out.println("started concurrency");
			executorService.invokeAll(taskList);
			cdl.await();
		}
		catch (InterruptedException e)
		{
			System.out.println("failed"); // NOPMD
		}
	}

	@Override
	public Void call() throws Exception
	{

		try
		{

			Set<String> lockKeys1 = new HashSet<String>();
			lockKeys1.add("lock1");
			lockKeys1.add("lock2");
			lockKeys1.add("lock3");
			lockKeys1.add("lock4");
			lockKeys1.add("lock5");
			lockKeys1.add("lock6");
			LockUtil.lock(lockKeys1);
		}
		catch (Exception e)
		{}
		return null;
	}

	private void assertNumberOfChildren(int expectedNumber) throws Exception
	{
		String path = LockSettings.LOCK_BASE_PATH.stringValue();
		List<String> children = zooKeeper.getChildren().forPath(path);
		assertThat(children.size(), is(expectedNumber));
	}

	@Test
	public void lockMaintainTimeoutTest() throws Exception
	{
		LockSettings.LOCK_MAINTAIN_TIMEOUT.setValue(15);

		long lockTime = System.currentTimeMillis() / 1000;
		Lock lock = LockUtil.lock("LockName1");
		BlockingZookeeperLock blockingZookeeperLock = (BlockingZookeeperLock) lock;
		zooKeeper.checkExists().usingWatcher(new LockWatcher()).forPath(blockingZookeeperLock.getLockId());
		cdl.await();
		if ((lockReleasedTime - lockTime) < 15)
		{
			throw new Exception("LOCK MAINTAIN TIMEOUT IS FAILING");
		}

	}

	private class LockWatcher implements CuratorWatcher
	{
		public void process(WatchedEvent event)
		{
			// lets either become the leader or watch the new/updated node
			lockReleasedTime = System.currentTimeMillis() / 1000;
			cdl.countDown();
		}
	}

}
