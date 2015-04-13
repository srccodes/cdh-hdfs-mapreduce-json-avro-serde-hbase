package org.poc.avro.serde.AVROSerDe;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

public class JSONToAvro {
	
	/*public static void getAvroBlob(String jsonData, File avroSchemaFile) throws ParseException {
		JSONParser parser = new JSONParser();
		JSONObject js = (JSONObject) parser.parse(jsonData);
		
		Message mesg = new Message();
		mesg.setMesgid((String)js.get("mesgid"));
		
		Dtls dtls = new Dtls();
		
		JSONObject detail = (JSONObject)js.get("dtls");
		List<CharSequence> cc = (List<CharSequence>)detail.get("cc");
		dtls.setCc(cc);
		
		String sender = (String)detail.get("sender");
		String subject = (String)detail.get("subject");
		String timestamp = (String)detail.get("timestamp");
		
		dtls.setSender(sender);
		dtls.setSubject(subject);
		dtls.setTimestamp(timestamp);
		
		mesg.setDtls(dtls);
		
		System.out.println("CC : " + mesg.getDtls().getCc());
		System.out.println("Timestamp : " + mesg.getDtls().getTimestamp());
		
		try {
			Schema schema = new Schema.Parser().parse(avroSchemaFile);
			System.out.println("Schema obtained...." + schema.getNamespace());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//DatumWriter<Message> messageDatumWriter = new SpecificDatumWr 
	}
	*//**
	 * @param json -- actual JSON data to be serialized
	 * @param avroSchemaFile -- Avro schema
	 * @return -- output Avro blob
	 * Using GeneraicDatumReader and GenericDatumWriter
	 * Only Avro schema dependent; no data dependency
	 */
	public static byte[] jsonToAvro(String json, File avroSchemaFile) {
	    InputStream input = null;
	    DataFileWriter<Object> writer = null;
	    ByteArrayOutputStream output = null;
	    try {
	        Schema schema = new Schema.Parser().parse(avroSchemaFile);
	        DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
	        input = new ByteArrayInputStream(json.getBytes());
	        output = new ByteArrayOutputStream();
	        DataInputStream din = new DataInputStream(input);
	        writer = new DataFileWriter<Object>(new GenericDatumWriter<Object>());
	        writer.create(schema, output);
	        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
	        Object datum;
	        while (true) {
	            try {
	                datum = reader.read(null, decoder);
	            } catch (EOFException eofe) {
	                break;
	            }
	            writer.append(datum);
	        }
	        writer.flush();
	        return output.toByteArray();
	    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
	        try { 
	        	input.close(); 
	        	writer.close();
	        } catch (Exception e) {}
	    }
	    return null;
	}
}
