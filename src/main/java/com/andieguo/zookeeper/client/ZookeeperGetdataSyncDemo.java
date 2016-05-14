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
 * @Description 同步获取znode的数据
 * @date 2016年5月14日 下午8:29:35  
 * @version V1.0    
 */
public class ZookeeperGetdataSyncDemo implements Watcher{

	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
	private static ZooKeeper zookeeper;
	private static Stat stat = new Stat();
	public static void main(String[] args) {
		try {
			String path = "/zk-book-sync-data";
			zookeeper = new ZooKeeper("115.29.110.73:2181", 5000, new ZookeeperGetdataSyncDemo());
			connectedSemaphore.await();
			//创建持久化节点
			zookeeper.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			//同步调用getData获取/zk-book-sync-data的内容，同时注册一个watch，之后一旦该节点内容发生变化，zk服务器会向客户端发送该节点的数据变更通知
			byte[] dat = zookeeper.getData(path, true,stat);
			System.out.println("data:"+new String(dat));
			zookeeper.setData(path, "345".getBytes(), -1);
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
			}else if(event.getType() == EventType.NodeDataChanged){
				//客户端收到节点数据变更通知后，再次同步调用getData获取最新的节点数据，同时注册watch
				//特别注意：Watcher通知是一次性的，即一旦触发一次通知后，该watch就失效了，因此该客户端需反复注册
				try {
					byte[] dat = zookeeper.getData(event.getPath(),true,stat);
					System.out.println("data:"+new String(dat));
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
