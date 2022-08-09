package main;

public class ObjectWrapper {
	private String key;
	private String value;
	private long time;
	
	public ObjectWrapper(String key, String value) {
		this.key = key;
		this.value = value;
		this.time = 0L;
	}
	
	public ObjectWrapper(String key, String value, long time) {
		this.key = key;
		this.value = value;
		this.time = time;
	}

	/**
	 * @return the index
	 */
	public String getKey() {
		return key;
	}

	/**
	 * @param index the index to set
	 */
	public void setKey(String key) {
		this.key = key;
	}

	/**
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * @return the time
	 */
	public long getTime() {
		return time;
	}

	/**
	 * @param time the time to set
	 */
	public void setTime(long time) {
		this.time = time;
	}

}
