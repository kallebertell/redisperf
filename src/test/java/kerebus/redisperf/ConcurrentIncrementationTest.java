package kerebus.redisperf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Quick and dirty incrementation tests for redis.
 * 
 * Included a pure in vm java incrementation test for comparison. 
 */
public class ConcurrentIncrementationTest extends AbstractTest {

	final static int CONCURRENT_CLIENTS = 100;
	final static int INCREMENTATION_TIMES = 1000;
	
	@Test
	public void a_lot_of_concurrent_vm_incrementing() throws Exception {
		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch completionCounter = new CountDownLatch(CONCURRENT_CLIENTS);
				
		AtomicLong valueToIncrement = new AtomicLong(0);
		
		for (int i=0; i<CONCURRENT_CLIENTS; i++) {
			new Thread(newVmIncrementer(valueToIncrement, INCREMENTATION_TIMES, startLatch, completionCounter), "Incrementer-"+i).start();
		}
		
		long startTime = System.currentTimeMillis();
		
		startLatch.countDown();
		
		System.out.println("Waiting for threads to finish.");
		
		completionCounter.await();

		long diff = System.currentTimeMillis() - startTime;

		System.out.println(CONCURRENT_CLIENTS + " concurrent clients incremented "+ INCREMENTATION_TIMES + " times each. Completed in " + diff + " ms.");
		double incrPerSecond = (CONCURRENT_CLIENTS*INCREMENTATION_TIMES) / (diff/1000d);
		System.out.println("So around "+incrPerSecond+" increments per second");
	}
	
	@Test
	public void a_lot_of_concurrent_redis_incrementing() throws Exception {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(CONCURRENT_CLIENTS);
		JedisPool pool = new JedisPool(config, getRedisHostname());
		
		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch completionCounter = new CountDownLatch(CONCURRENT_CLIENTS);
		
		String keyToIncrement = generateKey();
		
		for (int i=0; i<CONCURRENT_CLIENTS; i++) {
			new Thread(newRedisIncrementer(keyToIncrement, INCREMENTATION_TIMES, pool, startLatch, completionCounter), "Incrementer-"+i).start();
		}
		
		long startTime = System.currentTimeMillis();
		
		startLatch.countDown();
		
		System.out.println("Waiting for threads to finish.");
		
		completionCounter.await();

		long diff = System.currentTimeMillis() - startTime;

		print(CONCURRENT_CLIENTS + " concurrent clients incremented '" + keyToIncrement + "' "+ INCREMENTATION_TIMES + " times each. Completed in " + diff + " ms.");
		double incrPerSecond = (CONCURRENT_CLIENTS*INCREMENTATION_TIMES) / (diff/1000d);
		print("So around "+incrPerSecond+" increments per second");
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
				
				print("started.");
				
				Long count = null;
				
				for (int i=0; i<incrementationTimes; i++) {
					count = jedis.incr(keyToIncrement);	
				}
				
				print("finished with count == "+count);

				pool.returnResource(jedis);
				completionCounter.countDown();
			}
		};
	}
	
	private Runnable newVmIncrementer(final AtomicLong valueToIncrement, final int incrementationTimes, final CountDownLatch startLatch, final CountDownLatch completionCounter) {
		return new Runnable() {
			@Override public void run() {
				
				try {
					startLatch.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				
				print(" started.");
				
				Long count = null;
				
				for (int i=0; i<incrementationTimes; i++) {
					count = valueToIncrement.incrementAndGet();
				}
				
				print("finished with count == "+count);

				completionCounter.countDown();
			}
		};		
	}
	
	private static String generateKey() {
		return "incrKey" + System.currentTimeMillis();
	}
}
