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
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * a test class for JobServerClientImpl
 */
public class JobServerClientTest {
	private JobServerClient client = JobServerClient.builder().host("slc09woc.us.oracle.com").port(8090).build();
	//private JobServerClient client = JobServerClient.builder().host("h21").port(8090).build();
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}


	//@Test
	public void listJobs() throws Exception {
		List<JobInfo> jobs = client.getJobs();		
		for(JobInfo job: jobs){
			if(job.isFinished()){
				JobInfo r = client.getJobResult(job.getJobId());
				System.out.println(r);
			}
			else	
				System.out.println(job);
			
			JobConfig config = client.getConfig(job.getJobId());
			System.out.println(config);
		}
	}
	
	//@Test
	public void listBinaries() throws Exception {
		Binaries bins = client.getBinaries();
		for(String name: bins.keySet()){
			System.out.println(name + ":" + bins.get(name));
		}
	}
	
	//@Test
	public void createContext() throws Exception {
		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_NUM_CPU_CORES, "2");
		String result = client.createContext("context1", params);
		Assert.assertTrue(result.contains("context1"));
	}
	
	//@Test
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
	public void testRunJobWithFile() throws Exception {
		InputStream jarFileStream = ClassLoader.getSystemResourceAsStream("./job-server-tests.jar");
		File inputData = new File(ClassLoader.getSystemResource("input.json").toURI());

		String appName = "runjob-with-file-test";
		String str =client.uploadJobJar(jarFileStream, appName);
		Assert.assertEquals("OK", str);

		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_APP_NAME, appName);
		params.put(Constants.PARAM_CLASS_PATH, "spark.jobserver.WordCountExample");

		JobInfo result = client.startJob(inputData, params);
		while (result.isRunning()) {
			TimeUnit.SECONDS.sleep(1);
			result = client.getJobResult(result.getJobId());
		}

		Assert.assertEquals(4, result.getResult().get("a").getAsInt());
	}

	/**
	 * Warning: This test require deleting jar after test.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testUploadJar() throws Exception {
		InputStream jarFileStream = ClassLoader.getSystemResourceAsStream("./job-server-tests.jar");
		String appName = "upload-jar-test";
		String str = client.uploadJobJar(jarFileStream, appName);
		Assert.assertEquals("OK", str);

		//run it
		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_APP_NAME, appName);
		params.put(Constants.PARAM_CLASS_PATH, "spark.jobserver.WordCountExample");
		params.put(Constants.PARAM_SYNC, "true");		
		JobInfo result = client.startJob("input.string= fdsafd dfsf a b c a a a ", params);
		Assert.assertEquals(4, result.getResult().get("a").getAsLong());
		System.out.println(result);
	}

	//@Test
	public void test2() throws Exception {
		File jarFile = new File("d:/scratch/llian/workspace/job-server-tests/target/job-server-test-1.0.jar");
		String appName = "test2";
		String str = client.uploadJobJar(jarFile, appName);
		Assert.assertEquals("OK", str);

		Map<String, String> params = new HashMap<String, String>();
		params.put(Constants.PARAM_APP_NAME, appName);
		params.put(Constants.PARAM_CLASS_PATH, "spark.jobserver.JavaHelloWorldJob");
		params.put(Constants.PARAM_SYNC, "true");		
		JobInfo job = client.startJob("input.string = fdsafd dfsf a b c a a a ", params);
		System.out.println(job);
	}
}