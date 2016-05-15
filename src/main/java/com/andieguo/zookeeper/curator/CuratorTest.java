package com.andieguo.zookeeper.curator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import junit.framework.TestCase;

/**    
 * @author andieguo andieguo@foxmail.com
 * @Description 1、curator创建会话、同步创建节点、同步删除节点、同步读取数据、同步更新数据等操作测试用例 ;2)curator异步创建节点、事件监听节点数据发生变更、事件监听数据节点的子节点变化
 * @date 2016年5月15日 下午12:16:42  
 * @version V1.0    
 */
public class CuratorTest extends TestCase{

	private CuratorFramework client;
	@Override
	protected void setUp() throws Exception {
		// TODO Auto-generated method stub
		super.setUp();
		//使用Fluent风格的API接口创建会话
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		client = CuratorFrameworkFactory.builder()
				.connectString("115.29.110.73:2181")
				.sessionTimeoutMs(5000)
				.retryPolicy(retryPolicy)
				.build();
		client.start();
		Thread.sleep(1000);
	}
	/**
	 * 创建会话
	 */
	public void testCreateSession() throws InterruptedException{
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = 
				CuratorFrameworkFactory.newClient("115.29.110.73:2181",5000,3000,retryPolicy);
		client.start();
		Thread.sleep(1000);
	}
	/**
	 * 同步创建节点
	 */
	public void testCreate(){
		try {
			//creatingParentsIfNeeded接口，curator能够自动递归创建所需要的父节点
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
				.forPath("/zk-book-curator-1/clildren1/grandchild","123".getBytes());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
	}
	/**
	 * 同步删除节点
	 */
	public void testDelete(){
		Stat stat = new Stat();
		String path = "/zk-book-curator";
		try {
			//通过传入旧的stat来存储服务器端返回的最新的节点状态信息
			client.getData().storingStatIn(stat).forPath(path);
			//guranteed接口是一个保障措施，只要客户端会话有效，那么curator会在后台持续进行删除操作，直到节点删除成功
			//deletingChildrenIfNeeded接口，递归删除其所有子节点
			client.delete().guaranteed().deletingChildrenIfNeeded().withVersion(stat.getVersion()).forPath(path);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 同步获取节点数据
	 */
	public void testGetData(){
		String path = "/zk-book-curator/data1";
		try {
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path,"123".getBytes());
			Stat stat = new Stat();
			//通过传入旧的stat来存储服务器端返回的最新的节点状态信息
			byte[] data = client.getData().storingStatIn(stat).forPath(path);
			System.out.println("data:"+new String(data));
			System.out.println("stat:"+stat);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 同步更新节点数据
	 */
	public void testSetData(){
		String path = "/zk-book-curator/data3";
		try {
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path,"123".getBytes());
			Stat stat = new Stat();
			//通过传入旧的stat来存储服务器端返回的最新的节点状态信息
			byte[] data = client.getData().storingStatIn(stat).forPath(path);
			System.out.println("data:"+new String(data));
			System.out.println("stat:"+stat);
			//更新操作
			stat = client.setData().withVersion(stat.getVersion()).forPath(path,"456".getBytes());
			data = client.getData().forPath(path);
			System.out.println("data:"+new String(data));
			System.out.println("stat:"+stat);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 异步创建节点
	 */
	public void testAsync(){
		String path = "/zk-book-curator-async-1";
		final CountDownLatch semaphore = new CountDownLatch(2);
		ExecutorService tp = Executors.newFixedThreadPool(2);
		try {
			//使用自定义的Executor,将复杂的事件处理放在一个专门的线程中处理
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
				.inBackground(new BackgroundCallback() {

					public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
						// TODO Auto-generated method stub
						System.out.println("Executor event["+event.getResultCode()+",type:"+event.getType()+"]");
						System.out.println("Executor Thread of processResult:"+Thread.currentThread().getName());
						semaphore.countDown();
					}
				},tp).forPath(path+"/child2","123".getBytes());
			//不使用自定义的Executor,所有的异步通知事件处理都是由EventThread这个线程处理的，保证了对事件处理的顺序性
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
				.inBackground(new BackgroundCallback() {
					
					public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
						// TODO Auto-generated method stub
						System.out.println("event["+event.getResultCode()+",type:"+event.getType()+"]");
						System.out.println("Thread of processResult:"+Thread.currentThread().getName());
						semaphore.countDown();
					}
				}).forPath(path+"/child1", "234".getBytes());
			semaphore.await();
			tp.shutdown();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 事件监听：节点数据发生变更
	 */
	public void testNodeCache(){
		String path = "/zk-book-curator/nodecache";
		try {
			//NodeCacheListener也可监听节点是否存在，当节点不存在时，Cache会在节点创建后触发NodeCacheListener?
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, "123".getBytes());
			//实例化NodeCache
			@SuppressWarnings("resource")
			final NodeCache nodeCache = new NodeCache(client, path, false);
			//开启监听,默认为false，如果设置为true，那么NodeCache在第一次启动时会立刻从zk服务器上读取对应节点的数据内容，并保存到NodeCache中
			nodeCache.start(true);
			nodeCache.getListenable().addListener(new NodeCacheListener() {
				//设置NodeCacheListener监听，当数据发生变化时，回调该方法
				public void nodeChanged() throws Exception {
					// TODO Auto-generated method stub
					System.out.println("Node data update,new data:"+new String(nodeCache.getCurrentData().getData()));
				}
			});
			//更新数据
			client.setData().forPath(path,"456".getBytes());
			Thread.sleep(1000);
			//删除节点
			client.delete().deletingChildrenIfNeeded().forPath(path);
			Thread.sleep(1000);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 事件监听：数据节点的子节点变化
	 */
	public void testPathChildrenCache(){
		String path = "/zk-book-curator/pathchildrenlist";
		@SuppressWarnings("resource")
		PathChildrenCache cache = new PathChildrenCache(client, path, true);
		try {
			cache.start(StartMode.POST_INITIALIZED_EVENT);
			cache.getListenable().addListener(new PathChildrenCacheListener() {

				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					// TODO Auto-generated method stub
					switch (event.getType()) {
					case CHILD_ADDED:	
						System.out.println("CHILD_ADDED:"+event.getData().getPath());
						break;
					case CHILD_UPDATED:
						System.out.println("CHILD_UPDATED:"+event.getData().getPath());
						break;
					case CHILD_REMOVED:
						System.out.println("CHILD_REMOVED:"+event.getData().getPath());
						break;
					default:
						break;
					}
				}
				
			});
			//特别注意：如果主节点是临时的话，就不能构建其子节点，故创建的Path节点必须是持久性节点
			client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path,"123".getBytes());
			Thread.sleep(1000);
			//添加子节点
			client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path+"/c1","234".getBytes());
			Thread.sleep(1000);
			//更新子节点
			client.setData().forPath(path+"/c1","456".getBytes());
			Thread.sleep(1000);
			//删除子节点
			client.delete().forPath(path+"/c1");
			Thread.sleep(1000);
			//删除当前节点，对节点本身的变更，并没有通知到客户端
			client.delete().forPath(path);
			Thread.sleep(1000);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
