package kerebus.redisperf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class PubSubTest extends AbstractTest {

	final AtomicInteger subscriberCounter = new AtomicInteger(0);
	final String CHANNEL = "foo";
	final int MESSAGES_TO_PUBLISH = 1000;
	final int SUBSCRIBER_AMOUNT = 10;
	
	@Test
	public void publishing_to_subscribers() throws InterruptedException {
		Jedis jedis = newJedisClient();
		
		CountDownLatch completionCounter = new CountDownLatch(SUBSCRIBER_AMOUNT * MESSAGES_TO_PUBLISH);
		
		for (int i=0; i<SUBSCRIBER_AMOUNT; i++) {
			startSubscriberInNewThread(completionCounter);
		}
		
		long startTime = System.currentTimeMillis();
		
		print("Publishing "+MESSAGES_TO_PUBLISH+" messages to "+SUBSCRIBER_AMOUNT+" subscribers.");
		
		for (int i=0; i<MESSAGES_TO_PUBLISH; i++) {
			jedis.publish(CHANNEL, "message"+i);			
		}
		
		print("Waiting for subscribers to receive all messages.");
		
		completionCounter.await();
		long diff = System.currentTimeMillis() - startTime;
		
		print("Published "+MESSAGES_TO_PUBLISH+" messages and confirmed receival in "+SUBSCRIBER_AMOUNT+" subscribers in " + diff + " ms.");
		double publishedMessagesPerSecond = MESSAGES_TO_PUBLISH / (diff/1000d);
		print("Throughput (to confirmed delivery) with "+SUBSCRIBER_AMOUNT+" subscribers was about "+publishedMessagesPerSecond+" published messages per second.");
		double receivedMessagesPerSecond = (MESSAGES_TO_PUBLISH * SUBSCRIBER_AMOUNT) / (diff/1000d);
		print(receivedMessagesPerSecond+" messages received in total per second.");
	}
	
	private void startSubscriberInNewThread(final CountDownLatch completionCounter) {
		new Thread(new Runnable() {
			@Override public void run() {
				startSubscriber(completionCounter);				
			}
		}, "Subscriber-"+subscriberCounter.incrementAndGet()).start();
	}
	
	private void startSubscriber(final CountDownLatch completionCounter) {
		Jedis jedis = newJedisClient();
		
		jedis.subscribe(new JedisPubSub() {
			@Override public void onUnsubscribe(String channel, int subscribedChannels) {
				print("onPUnsubscribe on "+channel);
			}
			
			@Override public void onSubscribe(String channel, int subscribedChannels) {
				print("onSubscribe on "+channel);
			}
			
			@Override public void onPUnsubscribe(String pattern, int subscribedChannels) {
				print("onPUnsubscribe on "+pattern);
			}
			
			@Override public void onPSubscribe(String pattern, int subscribedChannels) {
				print("onPSubscribe on "+pattern);
			}
			
			@Override public void onPMessage(String pattern, String channel, String message) {
				print("onPMessage received "+message+" on channel "+channel);
			}
			
			@Override public void onMessage(String channel, String message) {
				//print("onMessage received "+message+" on channel "+channel);
				completionCounter.countDown();
			}
			
		}, CHANNEL);
		
	}
	
}
