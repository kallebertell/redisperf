package kerebus.redisperf;

import redis.clients.jedis.Jedis;

public abstract class AbstractTest {

	public static Jedis newJedisClient() {
		return new Jedis(getRedisHostname());
	}
	
	public static String getRedisHostname() {
		return "localhost";
	}
	
	public static void print(String msg) {
		System.out.println(Thread.currentThread().getName() + ": " + msg);
	}
	
	public static void printSeparator() {
		System.out.println("============================================================================");
	}
	
	public static String formatDouble(double unformatted) {
		return String.format("%.2f", unformatted);
	}
	
}
