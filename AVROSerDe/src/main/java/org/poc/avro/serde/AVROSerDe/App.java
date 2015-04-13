package org.poc.avro.serde.AVROSerDe;

import java.io.File;

import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.parser.ParseException;

/**
 * Hello world!
 *
 */
public class App 
{
	static String inputJSON = "{" +
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
	public static void main( String[] args ) throws ParseException
    {
        System.out.println( "Hello World!" );
        //JSONToAvro.getAvroBlob(inputJSON, new File("src/main/avro/InboxJSON.avsc"));
		byte[] avroBlob = JSONToAvro.jsonToAvro(inputJSON, new File("src/main/avro/InboxJSON.avsc"));
		System.out.println("avroBlob : " + Bytes.toString(avroBlob));
		String json = AvroToJSON.avroToJson(avroBlob);
		System.out.println("JSON : " + json);
    }
}
