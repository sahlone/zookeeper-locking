package com.zycus.zookeeper.lock;

import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

/**
 * @author sahil.lone
 *
 */
public enum LockSettings
{
	/**
	 * SESSION_TIMEOUT in seconds
	 */
	SESSION_TIMEOUT(600),
	/**
	 * ZK_CONNECTION_TIMEOUT in seconds
	 */
	ZK_CONNECTION_TIMEOUT(10),
	/**
	 * ZK_CONNECTION_STRING
	 */
	ZK_CONNECTION_STRING(),
	/**
	 * LOCK_ACQUIRE_TIMEOUT in seconds
	 */
	LOCK_ACQUIRE_TIMEOUT(3 * 60),
	/**
	 * LOCK_MAINTAIN_TIMEOUT in seconds
	 */
	LOCK_MAINTAIN_TIMEOUT(60),
	/**
	 * LOCK_ACQUIRE_EXCEPTION
	 */
	LOCK_ACQUIRE_EXCEPTION("LOCK_ACQUIRE_TIMEOUT:Could't aquire lock after waiting for " + LOCK_ACQUIRE_TIMEOUT
			+ " seconds. Possibly another resource is holding the same."),
	/**
	 * LOCK_BASE_PATH
	 */
	LOCK_BASE_PATH("/APPLICATION-LOCKS"),
	/**
	 * DEFAULT_ACL
	 */
	DEFAULT_ACL(ZooDefs.Ids.OPEN_ACL_UNSAFE),
	/**
	 * LOCK_DESCRIMINATOR
	 */
	LOCK_DESCRIMINATOR("-"),
	/**
	 * LOCK_OWNER_IDENTIFICATION_BYTES
	 */
	LOCK_OWNER_IDENTIFICATION_BYTES("IS_OWNER".getBytes());

	private Object	value	= null;

	private LockSettings()
	{}

	private LockSettings(Object value)
	{
		this.setValue(value);
	}

	public int intValue()
	{
		return (int) this.getValue();
	}

	public Object getValue()
	{
		return value;
	}

	public void setValue(Object value)
	{
		this.value = value;
	}

	public String stringValue()
	{
		return value + "";
	}

	public List<ACL> listValue()
	{
		return (List<ACL>) value;
	}

	public int milliSecondsValue()
	{
		return ((int) this.getValue()) * 1000;
	}

	public byte[] getBytesData()
	{
		return (byte[]) value;
	}

}
