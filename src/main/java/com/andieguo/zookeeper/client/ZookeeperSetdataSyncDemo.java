package com.andieguo.zookeeper.client;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

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
 * @Description 同步更新znode的数据
 * @date 2016年5月14日 下午8:29:35  
 * @version V1.0    
 */
public class ZookeeperSetdataSyncDemo implements Watcher{

	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
	private static ZooKeeper zookeeper;
	public static void main(String[] args) {
		try {
			String path = "/zk-book-sync-setdata-1";
			zookeeper = new ZooKeeper("115.29.110.73:2181", 5000, new ZookeeperSetdataSyncDemo());
			connectedSemaphore.await();
			//创建持久化节点
			zookeeper.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			//执行更新节点操作
			Stat stat = zookeeper.setData(path, "345".getBytes(), -1);
			System.out.println("stat:"+stat);
			//基于第一次更新的版本号执行更新，更新成功
			Stat stat1 = zookeeper.setData(path, "456".getBytes(), stat.getVersion());
			System.out.println("stat1"+stat1);
			//再次基于第一次更新的版本号执行更新，更新失败
			Stat stat2 = zookeeper.setData(path, "567".getBytes(), stat.getVersion());
			System.out.println("stat2"+stat2);
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

