package kerebus.redisperf;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Testing concurrent incrementation
 */
public class IncrementationTest extends AbstractTest {

	final static int CONCURRENT_CLIENTS = 100;
	final static int INCREMENTATION_TIMES = 1000;
	
	@Test
	public void a_lot_of_concurrent_incrementing() throws Exception {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(CONCURRENT_CLIENTS);
		JedisPool pool = new JedisPool(config, getRedisHostname());
		
		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch completionCounter = new CountDownLatch(CONCURRENT_CLIENTS);
		
		String keyToIncrement = generateKey();
		
		for (int i=0; i<CONCURRENT_CLIENTS; i++) {
			new Thread(newRedisIncrementer(keyToIncrement, INCREMENTATION_TIMES, pool, startLatch, completionCounter), "Incrementer-"+i).start();
		}
		
		print(CONCURRENT_CLIENTS + " clients incrementing the same key " + INCREMENTATION_TIMES + " times each.");
		
		long startTime = System.currentTimeMillis();
		startLatch.countDown();
		
		print("Waiting for threads to finish.");
		
		completionCounter.await();

		long diff = System.currentTimeMillis() - startTime;
		print(CONCURRENT_CLIENTS + " concurrent clients incremented '" + keyToIncrement + "' "+ INCREMENTATION_TIMES + " times each. Completed in " + diff + " ms.");
		
		double incrPerSecond = (CONCURRENT_CLIENTS*INCREMENTATION_TIMES) / (diff/1000d);
		print("So around " + formatDouble(incrPerSecond) + " increments per second");
	
		printSeparator();
	}

	private Runnable newRedisIncrementer(final String keyToIncrement, final int incrementationTimes, final JedisPool pool, final CountDownLatch startLatch, final CountDownLatch completionCounter) {
		return new Runnable() {
			@Override public void run() {
				Jedis jedis = pool.getResource();
				
				try {
					startLatch.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				
				for (int i=0; i<incrementationTimes; i++) {
					jedis.incr(keyToIncrement);	
				}

				pool.returnResource(jedis);
				completionCounter.countDown();
			}
		};
	}
	
	private static String generateKey() {
		return "incrKey" + System.currentTimeMillis();
	}
}
