package com.andieguo.zookeeper.client;

import java.io.IOException;
import java.util.List;
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
 * @Description 异步获取znode的孩子节点
 * @date 2016年5月14日 下午8:29:35  
 * @version V1.0    
 */
public class ZookeeperListASyncDemo implements Watcher{

	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
	private static ZooKeeper zookeeper;
	public static void main(String[] args) {
		try {
			String path = "/zk-book-async-list-5";
			zookeeper = new ZooKeeper("115.29.110.73:2181", 5000, new ZookeeperListASyncDemo());
			connectedSemaphore.await();
			//创建持久化节点
			zookeeper.create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			//异步调用getChildren获取/zk-book-list下的所有子节点，同时注册一个watch，一旦之后有子节点被创建，zk服务器会向客户端发送一个子节点变更通知
			zookeeper.getChildren(path, true,new IChildren2Callback(),"this is a context");
			//创建临时节点
			zookeeper.create(path+"/c1", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			zookeeper.create(path+"/c2", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			zookeeper.create(path+"/c3", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
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
			}else if(event.getType() == EventType.NodeChildrenChanged){
				//客户端收到子节点变更通知后，再次异步调用getChildren获取最新的子节点列表，同时注册watch
				//特别注意：Watcher通知是一次性的，即一旦触发一次通知后，该watch就失效了，因此该客户端需反复注册
				zookeeper.getChildren(event.getPath(), true,new IChildren2Callback(),"this is a context");
			}
		}
	}

}

class IChildren2Callback implements AsyncCallback.Children2Callback{

	public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
		// TODO Auto-generated method stub
		System.out.println("Get Children List:[rc:"+rc+",path:"+path+",ctx:"+ctx+",children:"+children+",stat:"+stat+"]");
	}
	
}