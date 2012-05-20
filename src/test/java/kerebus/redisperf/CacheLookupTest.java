package kerebus.redisperf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import redis.clients.jedis.Jedis;

/**
 * Simulates concurrent cache lookups for a single popular cached value. 
 */
public class CacheLookupTest extends AbstractTest {

	AtomicInteger clientCounter = new AtomicInteger(0);
	
	final int CONCURRENT_CLIENTS = 100;
	final int CACHE_LOOKUPS_PER_CLIENT = 1000;
	
	@Test
	public void concurrent_clients_fetches_stuff_from_cache() throws InterruptedException {
		Jedis jedis = newJedisClient();
		
		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch completionCounter = new CountDownLatch(CONCURRENT_CLIENTS);
		
		String cacheKey = generateCacheKey();
		
		jedis.set(cacheKey, "cached value");
		
		for (int i=0; i<CONCURRENT_CLIENTS; i++) {
			startClient(cacheKey, startLatch, completionCounter);
		}
		
		long startTime = System.currentTimeMillis();
		
		print("Starting cache lookups.");
		startLatch.countDown();
		
		print("Waiting for clients to finish lookups.");
		completionCounter.await();
	
		long diff = System.currentTimeMillis() - startTime;
		print(CONCURRENT_CLIENTS + " clients did " + CACHE_LOOKUPS_PER_CLIENT + " lookups each for a single cached value in " + diff + " ms.");
		
		double lookupsPerSecond = (CONCURRENT_CLIENTS * CACHE_LOOKUPS_PER_CLIENT) / (diff / 1000d);
		print("Served about " + lookupsPerSecond + " lookups per second.");
	}
	
	private void startClient(final String key, final CountDownLatch startLatch, final CountDownLatch completionCounter) {
		new Thread(new Runnable() {
			@Override public void run() {
				Jedis jedis = newJedisClient();
				
				try {
					startLatch.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				
				for (int i=0; i<CACHE_LOOKUPS_PER_CLIENT; i++) {
					jedis.get(key);
				}
				
				completionCounter.countDown();
				
				jedis.disconnect();
			}
		}, "CacheClient-"+clientCounter.incrementAndGet()).start();
	}
	
	private String generateCacheKey() {
		return "cacheKey" + System.currentTimeMillis();
	}
	
}
