package com.andieguo.zookeeper.client;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

/**    
 * @author andieguo andieguo@foxmail.com
 * @Description 同步创建znode节点
 * @date 2016年5月14日 下午5:03:10  
 * @version V1.0    
 */
public class ZookeeperCreateSyncDemo implements Watcher{
	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

	public static void main(String[] args) {
		try {
			ZooKeeper zookeeper = new ZooKeeper("115.29.110.73:2181", 5000, new ZookeeperCreateSyncDemo());
			connectedSemaphore.await();
			//创建临时节点
			String path1 = zookeeper.create("/zk-book-test-1", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println("success create znode:"+path1);
			//创建临时顺序节点
			String path2 = zookeeper.create("/zk-book-test-2", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("success create znode:"+path2);
			//创建持久化节点
			String path3 = zookeeper.create("/zk-book-test-3", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("success create znode:"+path3);
			//创建持久化顺序节点
			String path4 = zookeeper.create("/zk-book-test-4", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			System.out.println("success create znode:"+path4);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
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
