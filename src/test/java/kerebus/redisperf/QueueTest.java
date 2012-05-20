package kerebus.redisperf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import redis.clients.jedis.Jedis;

public class QueueTest extends AbstractTest {

	volatile boolean itemsLeftInQueue = true;
	
	AtomicInteger queueListenerCounter = new AtomicInteger(0);

	final int QUEUE_ITEM_AMOUNT = 50000;
	final int QUEUE_LISTENER_AMOUNT = 5;
	
	@Test
	public void queue_and_dequeue_with_several_concurrent_listeners() throws InterruptedException {
		Jedis jedis = new Jedis("localhost");
		
		String key = generateKey();
		
		for (int i=0; i<QUEUE_ITEM_AMOUNT; i++) {
			jedis.rpush(key, "foo"+i);
		}

		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch completionCounter = new CountDownLatch(QUEUE_ITEM_AMOUNT);
		
		for (int i=0; i<QUEUE_LISTENER_AMOUNT; i++) {
			startQueueListenerInNewThread(key, startLatch, completionCounter);
		}
		
		print("Starting queue listening.");
		
		long startTime = System.currentTimeMillis();
		
		startLatch.countDown();
		
		print("Waiting for all items to be dequeued.");
		
		completionCounter.await();
		itemsLeftInQueue = false;
		
		long diff = System.currentTimeMillis() - startTime;
	
		// This is kind of fucked. blpop doesn't throw interrupted exception so we have get them out of blocking mode like this
		for (int i=0; i<QUEUE_LISTENER_AMOUNT; i++) {
			jedis.rpush(key, "breakListenersOutOfBlockingMode");
		}
		
		print(QUEUE_LISTENER_AMOUNT + " clients listening to '" + key + "' containing " + QUEUE_ITEM_AMOUNT + " items emptied the queue in " + diff + " ms");
		print("Queue listeners processed about " + (QUEUE_ITEM_AMOUNT/ (diff/1000d) ) +" items per second.");
	}
	
	private void startQueueListenerInNewThread(final String queueKey, final CountDownLatch startLatch, final CountDownLatch completionCounter) {
		new Thread(new Runnable() {
			private int itemsDequeued = 0;
			
			@Override public void run() {
				Jedis jedis = new Jedis("localhost");
				
				try {
					startLatch.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				
				while (itemsLeftInQueue) {
					jedis.blpop(0, queueKey);
					itemsDequeued++;
					completionCounter.countDown();
				}
				
				print("dequeued a total of " + itemsDequeued + " items");
			}
		}, "QueueListener-"+queueListenerCounter.incrementAndGet()).start();
	}
	
	private String generateKey() {
		return "queueKey" + System.currentTimeMillis();
	}

}
