package com.andieguo.zookeeper.client;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**    
 * @author andieguo andieguo@foxmail.com
 * @Description 异步更新znode的数据
 * @date 2016年5月14日 下午8:29:35  
 * @version V1.0    
 */
public class ZookeeperSetdataASyncDemo implements Watcher{

	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
	private static ZooKeeper zookeeper;
	public static void main(String[] args) {
		try {
			String path = "/zk-book-async-setdata-7";
			zookeeper = new ZooKeeper("115.29.110.73:2181", 5000, new ZookeeperSetdataASyncDemo());
			connectedSemaphore.await();
			//创建持久化节点
			zookeeper.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			//异步执行更新节点操作，设置version为-1，表示基于数据的最新版本号进行更新操作
			zookeeper.setData(path, "345".getBytes(), -1,new IStatCallback(),null);
			//异步执行更新节点操作，设置version为-1，表示基于数据的最新版本号进行更新操作
			zookeeper.setData(path, "456".getBytes(), -1,new IStatCallback(),null);
			//异步执行更新节点操作，设置version为-1，表示基于数据的最新版本号进行更新操作
			zookeeper.setData(path, "567".getBytes(), -1,new IStatCallback(),null);
			Thread.sleep(Integer.MAX_VALUE);
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
		if(KeeperState.SyncConnected == event.getState()){
			if(EventType.None == event.getType() && null == event.getPath()){
				connectedSemaphore.countDown();
			}
		}
	}

}

class IStatCallback implements AsyncCallback.StatCallback{

	public void processResult(int rc, String path, Object ctx, Stat stat) {
		// TODO Auto-generated method stub
		if(rc == 0){
			System.out.println("rc:"+rc+",path:"+path+",ctx:"+ctx+",stat:"+stat);
		}
	}
	
}

