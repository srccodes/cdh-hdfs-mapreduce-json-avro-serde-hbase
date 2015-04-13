package org.poc.json.parser.JSONParser;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        String json = "[" +
    
      "{" +
        "\"mesgid\": \"001\"," +
        "\"Dtls\": {" +
            "\"cc\": ["+
                "\"sc@abc.com\",\"sd@abc.com\",\"se@abc.com\""+
            "],"+
            "\"sender\": \"sou@abc.com\","+
            "\"timestamp\": \"123456\"," +
            "\"subject\": \"hello 1\"" +
        "}" +
    "}"; 
    /*"{" +
    "\"mesgid\": \"002\"," +
    "\"dtls\": {" +
        "\"cc\": ["+
            "\"sc@abc.com\",\"sd@abc.com\",\"se@abc.com\""+
        "],"+
        "\"sender\": \"sou1@abc.com\","+
        "\"timestamp\": \"123457\"," +
        "\"subject\": \"hello 2\"" +
    "}" +
"}," +
"]";*/
        try {
        	if(!json.startsWith("{")) {
        		json = json.substring(json.indexOf("{"), json.length());
        	}
        	if(!json.endsWith("}")) {
        		json = json.substring(0, json.lastIndexOf("}")+1);
        	}
			JSONObject js = (JSONObject) new JSONParser().parse(json);
			System.out.println("Timestamp : " + ((JSONObject)js.get("Dtls")).get("timestamp"));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
        
    }
}
