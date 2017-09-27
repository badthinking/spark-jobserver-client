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

import static util.Pojo.gson;

import java.io.*;
import java.util.*;

import com.google.gson.reflect.TypeToken;

import lombok.*;
import util.Http;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;


/**
 * Java client implements Rest APIs provided by <a href="https://github.com/ooyala/spark-jobserver">Spark
 *  Job Server</a>. For more information, refer to <a href="https://github.com/ooyala/spark-jobserver">
 *  https://github.com/ooyala/spark-jobserver</a>.  
 */
@Getter
@Setter
@Builder
public class JobServerClient {
	private String host;
	private int port;

	/**
	 * <p>
	 * This method implements the Rest API <code>'POST /binaries/&lt;appName&gt;' </code>
	 * of the  Job Server.
	 * 
	 * @param binStream Contents of the target jar file to be uploaded
	 * @param alias of the uploaded jar file.
	 * @return result.
	 * @throws IOException
	 */
	public String uploadJobJar(InputStream binStream, String appName) throws IOException {
		return Http.postJar(makeUrl("/binaries/" + appName), IOUtils.toByteArray(binStream));
	}

	/**
	 * Uploads a jar containing spark job to the  Job Server under
	 * the given application name.
	 * 
	 * <p>
	 * This method implements the Rest API <code>'POST /binaries/&lt;appName&gt;' </code>
	 * of the  Job Server.
	 * 
	 * @param binFile the binary file
	 * @param appName the application name under which the related  Job
	 *     is about to run, meanwhile the application name also be the alias
	 *     name of the uploaded jar file.
	 * @return true if the operation of uploading is successful, false otherwise
	 * @throws JobServerClientException if the given parameter jarData or
	 *     appName is null, or error occurs when uploading the related spark job 
	 *     jar
	 */
	public String uploadJobJar(File binFile, String appName) throws IOException {
		return uploadJobJar(new FileInputStream(binFile), appName);
	}

	/**
	 * Lists all the contexts available in the  Job Server.
	 * 
	 * <p>
	 * This method implements the Rest API <code>'GET /contexts '</code>
	 * of the  Job Server.
	 * 
	 * @return a list containing names of current contexts
	 * @throws JobServerClientException error occurs when trying to get 
	 *         information of contexts
	 */
	public List<String> getContexts() throws IOException {
		String json = Http.get(makeUrl("/contexts"));
		return gson.fromJson(json, new TypeToken<ArrayList<String>>() {
		}.getType());
	}

	/**
	 * Creates a new context in the  Job Server with the given context name.
	 * 
	 * <p>
	 * This method implements the Rest API <code>'POST /contexts/&lt;name&gt;' </code>
	 * of the  Job Server.
	 * 
	 * @param contextName the name of the new context to be created, it should be not null
	 *        and should begin with letter.
	 * @param params a map containing the key-value pairs appended to appoint the context 
	 *        settings if there is a need to configure the new created context, or null indicates
	 *        the new context with the default configuration
	 * @return true if the operation of creating is successful, false it failed to create
	 *        the context because a context with the same name already exists
	 * @throws JobServerClientException when the given contextName is null or empty string,
	 *        or I/O error occurs while trying to create context in spark job server.
	 */
	public String createContext(String contextName, Map<String, String> params) throws IOException {
		return Http.postJson(makeUrl("/contexts/" + contextName, params), "");
	}

	/**
	 * Delete a context with the given name in the  Job Server.
	 * All the jobs running in it will be stopped consequently.
	 * 
	 * <p>
	 * This method implements the Rest API <code>'DELETE /contexts/&lt;name&gt;' </code>
	 * of the  Job Server.
	 * 
	 * @param contextName the name of the target context to be deleted, it should be not null
	 *        and should begin with letter.
	 * @return true if the operation of the deleting is successful, false otherwise
	 * @throws JobServerClientException when the given contextName is null or empty string,
	 *        or I/O error occurs while trying to delete context in spark job server.
	 */
	public String deleteContext(String contextName) throws IOException {
		return Http.delete(makeUrl("/contexts/" + contextName));
	}

	/**
	 * Lists the last N jobs in the  Job Server.
	 * 
	 * <p>
	 * This method implements the Rest API <code>'GET /jobs' </code> of the 
	 * Job Server.
	 * 
	 * @return a list containing information of the jobs
	 * @throws JobServerClientException error occurs when trying to get 
	 *         information of jobs
	 */
	public List<JobInfo> getJobs() throws IOException {
		String json = Http.get(makeUrl("/jobs"));
		return gson.fromJson(json, new TypeToken<ArrayList<JobInfo>>() {
		}.getType());
	}

	/**
	 * Start a new job with the given parameters.
	 * 
	 * <p>
	 * This method implements the Rest API <code>'POST /jobs' </code> of the 
	 * Job Server.
	 * 
	 * @param data contains the the data processed by the target job.
	 * 		 <p>
	 * 	     If it is null, it means the target spark job doesn't needs any data set
	 *       in the job configuration.
	 * 		 <p>
	 * 	     If it is not null, the format should be like a key-value pair, such as 
	 * 	     <code>dataKey=dataValue</code>, what the dataKey is determined by the 
	 * 		 one used in the target spark job main class which is assigned by 
	 *       IJobServerClientConstants.PARAM_CLASS_PATH.
	 * @param params a non-null map containing parameters to start the job.
	 *       the key should be the following ones:
	 *       i. <code>IJobServerClientConstants.PARAM_APP_NAME</code>, necessary 
	 *       one and should be one of the existing name in the calling of <code>GET /jars</code>.
	 *       That means the appName is the alias name of the uploaded spark job jars.
	 *
	 *       ii.<code>IJobServerClientConstants.PARAM_CLASS_PATH</code>, necessary one
	 *
	 *       iii.<code>IJobServerClientConstants.PARAM_CONTEXT</code>, optional one
	 *
	 *       iv.<code>IJobServerClientConstants.PARAM_SYNC</code>, optional one
	 *
	 * @return the corresponding job status or job result
	 * @throws JobServerClientException the given parameters exist null or empty value,
	 *        or I/O error occurs when trying to start the new job
	 */
	public JobInfo startJob(String data, Map<String, String> params) throws IOException {
		String json = Http.postJson(makeUrl("/jobs", params), data);
		return gson.fromJson(json, JobInfo.class);
	}

	/**
	 * Start a new job with the given parameters.
	 *
	 * <p>
	 * This method implements the Rest API <code>'POST /jobs' </code> of the 
	 * Job Server.
	 *
	 * @param dataFileStream contains the the data processed by the target job.
	 * 		 <p>
	 * 	     If it is null, it means the target spark job doesn't needs any data set
	 *       in the job configuration.
	 * 		 <p>
	 * 	     If it is not null, the format should be Typesafe Config Style, such as
	 * 	     json, properties file etc. See <a href="http://github.com/typesafehub/config">http://github.com/typesafehub/config</a>
	 * 	     what the keys in the file are determined by the
	 * 		 one used in the target spark job main class which is assigned by
	 *       IJobServerClientConstants.PARAM_CLASS_PATH.
	 * @param params a non-null map containing parameters to start the job.
	 *       the key should be the following ones:
	 *       i. <code>IJobServerClientConstants.PARAM_APP_NAME</code>, necessary
	 *       one and should be one of the existing name in the calling of <code>GET /jars</code>.
	 *       That means the appName is the alias name of the uploaded spark job jars.
	 *
	 *       ii.<code>IJobServerClientConstants.PARAM_CLASS_PATH</code>, necessary one
	 *
	 *       iii.<code>IJobServerClientConstants.PARAM_CONTEXT</code>, optional one
	 *
	 *       iv.<code>IJobServerClientConstants.PARAM_SYNC</code>, optional one
	 *
	 * @return the corresponding job status or job result
	 * @throws JobServerClientException the given parameters exist null or empty value,
	 *        or I/O error occurs when trying to start the new job
	 */
	public JobInfo startJob(InputStream dataFileStream, Map<String, String> params) throws IOException {
		return startJob(IOUtils.toString(dataFileStream), params);
	}

	/**
	 * Start a new job with the given parameters.
	 *
	 * <p>
	 * This method implements the Rest API <code>'POST /jobs' </code> of the 
	 * Job Server.
	 *
	 * @param dataFile contains the the data processed by the target job.
	 * 		 <p>
	 * 	     If it is null, it means the target spark job doesn't needs any data set
	 *       in the job configuration.
	 * 		 <p>
	 * 	     If it is not null, the format should be Typesafe Config Style, such as
	 * 	     json, properties file etc. See <a href="http://github.com/typesafehub/config">http://github.com/typesafehub/config</a>
	 * 	     what the keys in the file are determined by the
	 * 		 one used in the target spark job main class which is assigned by
	 *       IJobServerClientConstants.PARAM_CLASS_PATH.
	 * @param params a non-null map containing parameters to start the job.
	 *       the key should be the following ones:
	 *       i. <code>IJobServerClientConstants.PARAM_APP_NAME</code>, necessary
	 *       one and should be one of the existing name in the calling of <code>GET /jars</code>.
	 *       That means the appName is the alias name of the uploaded spark job jars.
	 *
	 *       ii.<code>IJobServerClientConstants.PARAM_CLASS_PATH</code>, necessary one
	 *
	 *       iii.<code>IJobServerClientConstants.PARAM_CONTEXT</code>, optional one
	 *
	 *       iv.<code>IJobServerClientConstants.PARAM_SYNC</code>, optional one
	 *
	 * @return the corresponding job status or job result
	 * @throws JobServerClientException the given parameters exist null or empty value,
	 *        or I/O error occurs when trying to start the new job
	 */
	public JobInfo startJob(File dataFile, Map<String, String> params) throws IOException {
		return startJob(FileUtils.readFileToString(dataFile), params);
	}

	public String killJob(String jobId) throws IOException {
		return Http.delete(makeUrl("jobs/" + jobId));
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws Exception
	 * @throws IOException
	 */
	public JobInfo getJobResult(String jobId) throws IOException {
		String json = Http.get(makeUrl("/jobs/" + jobId));
		final JobInfo jobResult = gson.fromJson(json, JobInfo.class);
		jobResult.setJobId(jobId);
		return jobResult;
	}

	/**
	 * Gets the job configuration of a specific job.
	 * 
	 * <p>
	 * This method implements the Rest API <code>'GET /jobs/&lt;jobId&gt;/config' </code>
	 * of the  Job Server.
	 * 
	 * @param jobId the id of the target job
	 * @return the corresponding <code>JobConfig</code> instance if the job
	 * with the given jobId exists, or null if there is no corresponding job in 
	 * the spark job server.
	 * @throws JobServerClientException error occurs when trying to get 
	 *         information of the target job configuration
	 */
	public JobConfig getConfig(String jobId) throws IOException {
		String json = Http.get(makeUrl("/jobs/" + jobId + "/config"));
		return gson.fromJson(json, JobConfig.class);
	}
	
	/**
	 * Implements the Rest API <code>'GET /binaries'</code> of the Job Server.
	 * 
	 * @return a Map containing jar name and uploaded date time.
	 * @throws IOException error occurs when trying to get information of spark job binaries
	 */
	public Binaries getBinaries() throws IOException{
		String json = Http.get(makeUrl("/binaries"));
		return gson.fromJson(json, Binaries.class);
	}

	public String deleteBinary(String name) throws IOException{
		return Http.delete(makeUrl("/binaries/" + name));
	}
	
	/**
	 * construct url with path
	 * @param path
	 * @return
	 */
	private String makeUrl(String path) {
		return makeUrl(path, null);
	}

	/**
	 * Construct url with path and parameters
	 * @param path
	 * @return
	 */
	private String makeUrl(String path, Map<String, String> args) {
		String url = StringUtils.join("http://", host, ":", port, path.startsWith("/") ? "": "/", path);
		if (args != null && !args.isEmpty()) {
			Optional<String> argstr = args.entrySet().stream().map(e -> StringUtils.join(e.getKey(), "=", e.getValue()))
					.reduce((a, b) -> a + "&" + b);
			url = StringUtils.join(url, "?", argstr.orElse(""));
		}
		return url;
	}	
}
