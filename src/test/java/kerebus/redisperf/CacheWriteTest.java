package kerebus.redisperf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import redis.clients.jedis.Jedis;

/**
 * Simulates writing to cache 
 */
public class CacheWriteTest extends AbstractTest {

	AtomicInteger clientCounter = new AtomicInteger(0);
	
	final int CONCURRENT_CLIENTS = 100;
	final int CACHE_WRITES_PER_CLIENT = 1000;
	
	@Test
	public void concurrent_clients_write_stuff_to_cache() throws InterruptedException {
		
		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch completionCounter = new CountDownLatch(CONCURRENT_CLIENTS);
		
		for (int i=0; i<CONCURRENT_CLIENTS; i++) {
			startClient(startLatch, completionCounter);
		}
		
		long startTime = System.currentTimeMillis();
		
		print("Starting cache writers.");
		startLatch.countDown();
		
		print("Waiting for clients to finish writes.");
		completionCounter.await();
	
		long diff = System.currentTimeMillis() - startTime;
		print(CONCURRENT_CLIENTS + " clients did " + CACHE_WRITES_PER_CLIENT + " writes in " + diff + " ms.");
		
		double lookupsPerSecond = (CONCURRENT_CLIENTS * CACHE_WRITES_PER_CLIENT) / (diff / 1000d);
		print("Served about " + lookupsPerSecond + " writes per second.");
	}
	
	private void startClient(final CountDownLatch startLatch, final CountDownLatch completionCounter) {
		new Thread(new Runnable() {
			@Override public void run() {
				Jedis jedis = newJedisClient();
				
				try {
					startLatch.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				
				for (int i=0; i<CACHE_WRITES_PER_CLIENT; i++) {
					String cacheKey =  Thread.currentThread().getName() + "-key-" + i;
					String cacheValue =  Thread.currentThread().getName() + "-value-" + i;
					jedis.set(cacheKey, cacheValue);
				}
				
				completionCounter.countDown();
				
				jedis.disconnect();
			}
		}, "CacheClient-"+clientCounter.incrementAndGet()).start();
	}
	
}
