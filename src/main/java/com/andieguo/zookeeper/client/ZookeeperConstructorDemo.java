package com.andieguo.zookeeper.client;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author andieguo andieguo@foxmail.com
 * @Description 连接zookeeper服务器
 * @date 2016年5月14日 下午4:52:43
 * @version V1.0
 */
public class ZookeeperConstructorDemo implements Watcher{
	
	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
	
	public static void main(String[] args) {
		try {
			ZooKeeper zookeeper = new ZooKeeper("115.29.110.73:2181", 5000, new ZookeeperConstructorDemo());
			System.out.println(zookeeper.getState());
			System.out.println(zookeeper.getSessionId());
			connectedSemaphore.await();
			System.out.println("Zookeeper session established.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("Receive watched event:"+ event.getState());
		if(KeeperState.SyncConnected == event.getState()){
			connectedSemaphore.countDown();
		}
	}
}
