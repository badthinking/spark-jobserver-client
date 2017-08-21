package spark.jobserver.client;

import java.util.Date;

import com.google.gson.annotations.SerializedName;

import lombok.*;
import util.Pojo;

@Getter @Setter
public class Binary extends Pojo{
	@SerializedName("binary-type")  
	private String binary_type;
	@SerializedName("upload-time")  
	private Date upload_time;
}
