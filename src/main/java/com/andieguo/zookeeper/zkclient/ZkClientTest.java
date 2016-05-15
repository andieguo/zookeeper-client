package com.andieguo.zookeeper.zkclient;

import java.util.List;

import junit.framework.TestCase;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;

/**    
 * @author andieguo andieguo@foxmail.com
 * @Description zkClient创建会话、创建节点、 删除节点、获取节点列表、获取节点数据、更新节点数据测试用例
 * @date 2016年5月15日 上午10:46:56  
 * @version V1.0    
 */
public class ZkClientTest extends TestCase{
	
	private ZkClient zkClient;

	@Override
	protected void setUp() throws Exception {
		// TODO Auto-generated method stub
		super.setUp();
		//建立session连接
		zkClient = new ZkClient("115.29.110.73:2181", 5000);
		System.out.println("zookeeper session established");
	}
	
	public void testCreate(){
		zkClient.create("/zk-book-1", "123", CreateMode.EPHEMERAL);
	}
	
	public void testCreatePersistent(){
		//设置createParent参数为true,表明可以递归建立父节点
		zkClient.createPersistent("/zk-book-1/child1", true);
	}
	
	public void testDelete(){
		//如果一个节点存在至少一个子节点，该节点利用delete方法无法直接删除该节点
		zkClient.delete("/zk-book-1");
	}
	
	public void testDeleteRecursive(){
		//递归删除节点下的所有子节点
		zkClient.deleteRecursive("zk-book-1");
	}
	
	public void testGetChildren() throws InterruptedException{
		String path = "/zk-book-list-test-4";
		//zkClient的Listener不是一次性的，客户端只需要注册一次就会一直生效
		zkClient.subscribeChildChanges(path, new IZkChildListener() {
			
			public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(parentPath+" 's child changed,currentChilds:"+currentChilds);
			}
		});
		//客户端可以对一个不存在的节点进行子节点变更监听
		zkClient.createPersistent(path);
		Thread.sleep(1000);
		zkClient.createPersistent(path+"/child1");
		Thread.sleep(1000);
		zkClient.createPersistent(path+"/child2");
		Thread.sleep(1000);
		zkClient.createPersistent(path+"/child3");
		Thread.sleep(1000);
		zkClient.delete(path+"/child3");
		Thread.sleep(1000);
		zkClient.delete(path+"/child2");
		Thread.sleep(1000);
		zkClient.delete(path+"/child1");
		Thread.sleep(1000);
		//该节点本身的创建或删除也会通知客户端
		zkClient.delete(path);
		Thread.sleep(1000);
	}
	
	public void testGetData() throws InterruptedException{
		String path = "/zk-book-list-test-4";
		zkClient.subscribeDataChanges(path, new IZkDataListener() {
			
			public void handleDataDeleted(String dataPath) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("Node "+dataPath+" deleted.");
			}
			
			public void handleDataChange(String dataPath, Object data) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("Node "+dataPath+"changed,new data:"+data);
			}
		});
		zkClient.createPersistent(path,"123");
		Thread.sleep(1000);
		zkClient.writeData(path, "234");
		Thread.sleep(1000);
		zkClient.delete(path);
		Thread.sleep(1000);
	}
}

