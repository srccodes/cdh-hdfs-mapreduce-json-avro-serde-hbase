package org.poc.mapred.helper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.poc.mapred.JSONToHBaseMapper;


public class JSONUtil {
	public static final Log LOG = LogFactory.getLog(JSONUtil.class);
	public static String formatJSON(String json) {
		json = json.trim();
    	if(!json.startsWith("{")) {
    		json = json.substring(json.indexOf("{"), json.length());
    	}
    	if(!json.endsWith("]")) {
    		json = json + "}";
    		
    	}
    	if(json.endsWith("]")) {
    		json = json.substring(0, json.lastIndexOf("}")+1);
    	}
    	
    	LOG.info("Formatted JSON : " + json);
		return json;
	}
	public static String getTimestampAndMesgId(String json) {
		String ts = null;
		String mesgId = null;
		try {
			JSONObject js = (JSONObject) new JSONParser().parse(json);
			mesgId = (String)js.get("mesgid");
			ts = (String)((JSONObject)js.get("Dtls")).get("timestamp");
			LOG.info("Timestamp : " + ts);
		} catch (org.json.simple.parser.ParseException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
		return ts + MapRedConstants.JSON_KEY_SEPARATOR + mesgId;
	}

}
