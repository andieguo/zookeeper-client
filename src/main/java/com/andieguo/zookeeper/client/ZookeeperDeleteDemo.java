package com.andieguo.zookeeper.client;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**    
 * @author andieguo andieguo@foxmail.com
 * @Description zookeeper删除znode节点
 * @date 2016年5月14日 下午5:03:10  
 * @version V1.0    
 */
public class ZookeeperDeleteDemo implements Watcher{
	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

	public static void main(String[] args) {
		try {
			ZooKeeper zookeeper = new ZooKeeper("115.29.110.73:2181", 5000, new ZookeeperDeleteDemo());
			connectedSemaphore.await();
			//同步删除
			//zookeeper.delete("/zk-book-test-async-40000000014", 0);
			//异步删除
			zookeeper.delete("/zk-book-test-40000000006",0,new IVoidCallback(),"i am context.");
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

class IVoidCallback implements AsyncCallback.VoidCallback{

	public void processResult(int rc, String path, Object ctx) {
		// TODO Auto-generated method stub
		System.out.println("rc:"+rc+",path:"+path+",ctx:"+ctx);
	}
}
