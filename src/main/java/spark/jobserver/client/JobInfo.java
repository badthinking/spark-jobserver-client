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

import java.util.Date;

import com.google.gson.JsonElement;

import lombok.Getter;
import lombok.Setter;
import util.Pojo;

/**
 * Presents the information of spark job result, when calling 
 * <code>GET /jobs/&lt;jobId&gt;</code> to a spark job server.
 * 
 */
@Getter @Setter
public class JobInfo extends Pojo {
	private String jobId;
	private JobStatus status;
	private String context;
	private String classPath;
	private String duration;
	private Date startTime;
	private JsonElement result;  //we do not know its class type 

	/**
	 * Judges current <code>JobResult</code> instance represents the 
	 * status information of a asynchronous running spark job or not.
	 * 
	 * @return true indicates it contains asynchronous running status of a
	 *         spark job, false otherwise
	 */
	public boolean isRunning() {
		return getStatus() == JobStatus.STARTED || getStatus() == JobStatus.RUNNING;		
	}

	public boolean isFinished(){
		return getStatus() == JobStatus.FINISHED || getStatus() == JobStatus.OK;		
	}
	
	/**
	 * If status is error.
	 * @return
	 */
	public boolean isError(){
		return status == JobStatus.ERROR;
	}		
	
	/**
	 * Judges the queried target job doesn't exist or not.
	 * 
	 * @return true indicates the related job doesn't exist, false otherwise
	 */
	public boolean jobNotExists() {
		return getStatus() == JobStatus.ERROR && getResult().toString().contains("No such job ID");
	}
}
