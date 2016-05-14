package com.andieguo.zookeeper.client;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

/**    
 * @author andieguo andieguo@foxmail.com
 * @Description 异步创建znode节点
 * @date 2016年5月14日 下午5:03:10  
 * @version V1.0    
 */
public class ZookeeperCreateAsyncDemo implements Watcher{
	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

	public static void main(String[] args) {
		try {
			ZooKeeper zookeeper = new ZooKeeper("115.29.110.73:2181", 5000, new ZookeeperCreateAsyncDemo());
			connectedSemaphore.await();
			//创建临时节点
			zookeeper.create("/zk-book-test-async-1", "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,new IStringCallback(),"I am context1.");
			//创建临时顺序节点
			zookeeper.create("/zk-book-test-async-2", "2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,new IStringCallback(),"I am context2.");
			//创建持久化节点
			zookeeper.create("/zk-book-test-async-3", "3".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,new IStringCallback(),"I am context3.");
			//创建持久化顺序节点
			zookeeper.create("/zk-book-test-async-4", "4".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL,new IStringCallback(),"I am context4.");
			//很关键，休眠
			Thread.sleep(Integer.MAX_VALUE);
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

class IStringCallback implements AsyncCallback.StringCallback{

	public void processResult(int rc, String path, Object ctx, String name) {
		// TODO Auto-generated method stub
		System.out.println("rc:"+rc+",path:"+path+",ctx:"+ctx+",name:"+name);
	}
	
}
