package org.toletum.pfm;

public interface Config {
	public static final String KafkaServer = "kafka:9092";
	public static final String KafkaTopicCrime = "crimes";
	public static final String KafkaTopicCommand = "command";
	public static final String KafkaTopicClock = "clock";

	public static final String ZooKeeperServer = "zookeeper:2181";
	
	public static final String RedisServer = "database";
	public static final String RedisCrimes = "Crimes";
	public static final String RedisClock = "Clock";
	public static final Integer RedisCrimesSize = new Integer(100000);
	
	public static final String RedisFuture = "Future";
	public static final String RedisFutureNext = "FutureNext";
	
	public static final String DatabaseDriver = "org.neo4j.jdbc.Driver";
	public static final String DatabaseServer = "jdbc:neo4j://database:7474/";
}
