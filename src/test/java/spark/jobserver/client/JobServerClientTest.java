/*
 * Copyright 2014-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.jobserver.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import spark.jobserver.client.Constants;
import spark.jobserver.client.JobInfo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.extern.log4j.Log4j;

/**
 * a test class for JobServerClientImpl
 */
@Log4j
public class JobServerClientTest {
	//private JobServerClient client = JobServerClient.builder().host("slc09woc.us.oracle.com").port(8090).build();	
	private JobServerClient client = JobServerClient.builder().host("master").port(8090).build();

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	// @Test
	public void listJobs() throws Exception {
		List<JobInfo> jobs = client.getJobs();
		for (JobInfo job : jobs) {
			if (job.isFinished()) {
				JobInfo r = client.getJobResult(job.getJobId());
				System.out.println(r);
			} else
				System.out.println(job);

			JobConfig config = client.getConfig(job.getJobId());
			System.out.println(config);
		}
	}

	// @Test
	public void listBinaries() throws Exception {
		Binaries bins = client.getBinaries();
		for (String name : bins.keySet()) {
			System.out.println(name + ":" + bins.get(name));
		}
	}

	// @Test
	public void createContext() throws Exception {
		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_NUM_CPU_CORES, "2");
		String result = client.createContext("context1", params);
		Assert.assertTrue(result.contains("context1"));
	}

	// @Test
	public void deleteContext() throws Exception {
		String result = client.deleteContext("context1");
		Assert.assertTrue(result.contains("SUCCESS"));
	}

	/**
	 * test runJob with File resource Warning: This test require deleting jar
	 * after test.
	 * 
	 * @throws Exception
	 */
	//@Test
	public void runJobWithFile() throws Exception {
		InputStream jarFileStream = ClassLoader.getSystemResourceAsStream("./job-server-tests.jar");
		File inputData = new File(ClassLoader.getSystemResource("input.json").toURI());

		String appName = "runjob-with-file-test";
		String str = client.uploadJobJar(jarFileStream, appName);
		Assert.assertEquals("OK", str);

		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_APP_NAME, appName);
		params.put(Constants.PARAM_CLASS_PATH, "test.jobserver.WordCountExample");

		JobInfo result = client.startJob(inputData, params);
		while (result.isRunning()) {
			TimeUnit.SECONDS.sleep(1);
			result = client.getJobResult(result.getJobId());
		}

		Assert.assertEquals(4, result.getResult().getAsJsonObject().get("a").getAsInt());
	}

	/**
	 * Warning: This test require deleting jar after test.
	 * 
	 * @throws Exception
	 */
	// @Test
	public void uploadJar() throws Exception {
		InputStream jarFileStream = ClassLoader.getSystemResourceAsStream("./job-server-tests.jar");
		String appName = "upload-jar-test";
		String str = client.uploadJobJar(jarFileStream, appName);
		Assert.assertEquals("OK", str);
		client.deleteBinary("merger2");
	}

	//@Test
	public void bingsAppRemote() throws Exception {
		File jarFile = new File("D:/scratch/llian/workspaces/spark/jars/bingsapp.jar");
		String appName = "bingqings-app";
		client.deleteBinary(appName);
		String str = client.uploadJobJar(jarFile, appName);
		Assert.assertEquals("OK", str);

		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_APP_NAME, appName);
		params.put(Constants.PARAM_CLASS_PATH, "com.bingqing.app_jobserver.BingsJob");
		params.put("spark.executor.memory", "1g");
		//params.put("spark.executor.instances", "3");
		params.put("spark.executor.cores", "1");

		JobInfo job = client.startJob("", params);
		System.out.println(job);
		for (int i = 0; i < 25; i++) {
			Thread.sleep(10000);
			job = client.getJobResult(job.getJobId());
			if (!job.isRunning())
				break;
			System.out.println("...");
		}
		log.info(job);

		client.killJob(job.getJobId());
	}
	
	@Test
	public void bingsAppLocal() throws Exception {
		File jarFile = new File("D:/scratch/llian/workspaces/spark/jars/bingsapp.jar");
		String appName = "bings-app2";
		client.deleteBinary(appName);
		String str = client.uploadJobJar(jarFile, appName);
		Assert.assertEquals("OK", str);

		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_APP_NAME, appName);
		params.put(Constants.PARAM_CLASS_PATH, "test.bing.BingsJob2");
		params.put("spark.executor.memory", "1g");
		//params.put("spark.executor.instances", "3");
		params.put("spark.executor.cores", "1");

		JobInfo job = client.startJob("interval=30,kafkaBootstrapServers=\"192.168.5.1:9092\",schemaFilePath=\"hdfs://master:9000/spark/apps/schema.xml\"", params);
		System.out.println(job);
		for (int i = 0; i < 25; i++) {
			Thread.sleep(10000);
			job = client.getJobResult(job.getJobId());
			if (!job.isRunning())
				break;
			System.out.println("...");
		}
		log.info(job);

		client.killJob(job.getJobId());
	}	
	
	//@Test
	public void merger4() throws Exception {
		File jarFile = new File("D:/scratch/llian/workspaces/spark/jars/demo.jar");
		String appName = "merger4";
		String ctxName = "kafka-streaming-context";// + (System.currentTimeMillis() % 100);
		// client.deleteContext(ctxName);

		client.deleteBinary(appName);
		String str = client.uploadJobJar(jarFile, appName);
		Assert.assertEquals("OK", str);

		Map<String, String> props = new HashMap<String, String>();
		props.put("context-factory", "spark.jobserver.context.StreamingContextFactory");
		props.put("streaming.batch_interval", "60000");
		props.put("streaming.stopSparkContext", "true");
		props.put("streaming.stopGracefully", "true");
		props.put("spark.executor.memory", "2g");
		props.put("spark.executor.instances", "1");
		props.put("spark.executor.cores", "1");
		client.createContext(ctxName, props);

		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_APP_NAME, appName);
		params.put(Constants.PARAM_CONTEXT, ctxName);
		params.put(Constants.PARAM_CLASS_PATH, "test.jobserver.MergerJob");

		JobInfo job = client.startJob("", params);
		System.out.println(job);
		for (int i = 0; i < 25; i++) {
			Thread.sleep(10000);
			job = client.getJobResult(job.getJobId());
			if (!job.isRunning())
				break;
			System.out.println("...");
		}
		log.info(job);

		//client.killJob(job.getJobId());
		//String s = "";
		//while (!s.contains("OK") && !s.contains("SUCCESS") && !s.contains("not found")) {
		//	s = client.deleteContext(ctxName);
		//}
	}

	//@Test
	public void sqlApp() throws Exception {
		File jarFile = new File("D:/scratch/llian/workspaces/spark/jars/demo.jar");
		String appName = "sql-test";
		String ctxName = "sql-context";
		client.deleteBinary(appName);

		client.deleteContext(ctxName);
		String str = client.uploadJobJar(jarFile, appName);
		Assert.assertEquals("OK", str);

		Map<String, String> props = new HashMap<String, String>();
		props.put("num-cpu-cores", "1");
		props.put("memory-per-node", "1g");
		props.put("context-factory", "spark.jobserver.context.SessionContextFactory");
		client.createContext(ctxName, props);

		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_APP_NAME, appName);
		params.put(Constants.PARAM_CONTEXT, ctxName);
		params.put(Constants.PARAM_CLASS_PATH, "test.jobserver.SessionJob");
		String s ="spark{ executor{memory=1g, cores=1}}";
		JobInfo job = client.startJob(s, params);
		System.out.println(job);
		while (job.isRunning()) {
			Thread.sleep(2000);
			job = client.getJobResult(job.getJobId());
		}

		client.deleteContext(ctxName);
	}

	// @Test
	public void syncMode() throws Exception {
		File jarFile = new File("d:/scratch/llian/workspace/job-server-tests/target/job-server-test-1.0.jar");
		String appName = "test2";
		String str = client.uploadJobJar(jarFile, appName);
		Assert.assertEquals("OK", str);

		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_APP_NAME, appName);
		params.put(Constants.PARAM_CLASS_PATH, "test.jobserver.JavaHelloWorldJob");
		params.put(Constants.PARAM_SYNC, "true");
		JobInfo job = client.startJob("input.string = fdsafd dfsf a b c a a a ", params);
		System.out.println(job);
	}

	// @Test
	public void yarnMetrics() throws Exception {

	}
	
	//@Test
	public void misc(){
		try {
			client.killJob("e7e98dc3-4291-463c-9122-27eddc03f5ce");
			client.deleteContext("kafka-streaming-context");
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
}