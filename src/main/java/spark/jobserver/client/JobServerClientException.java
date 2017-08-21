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

import lombok.*;

/**
 * The exception indicates errors occurs when using instance 
 * of <code>IJobServerClient</code>.
 */

@Getter
public class JobServerClientException extends Exception {	

	private static final long serialVersionUID = 1L;
	private int code;

	public JobServerClientException(int code, String message) {
		super(message);
		this.code = code;
	}
	
	public JobServerClientException(String message, Throwable cause) {
		super(message, cause);
	}
}
