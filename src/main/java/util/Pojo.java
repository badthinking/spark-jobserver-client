package util;

import java.lang.reflect.Type;

import com.google.gson.*;

public class Pojo {
	public final static transient Gson gson = new GsonBuilder().registerTypeAdapter(Double.class, new JsonSerializer<Double>() {  
		@Override  
        public JsonElement serialize(Double src, Type typeOfSrc, JsonSerializationContext context) {  
            if (src == src.longValue())  
                return new JsonPrimitive(src.longValue());  
            return new JsonPrimitive(src);  
        }
    }).setPrettyPrinting().create();
	
	@Override
	public String toString(){
		return gson.toJson(this);
	}
}
