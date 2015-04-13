package org.poc.avro.serde.AVROSerDe;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

public class AvroToJSON {
	/**
	 * @param avro -- byte array containing avro blob
	 * Using GeneraicDatumReader and GenericDatumWriter
	 * Schema is not required to be supplied from outside
	 * Serialize Avro blob contains schema inline
	 * @return -- JSON String
	 */
	public static String avroToJson(byte[] avro) {
	    boolean pretty = false;
	    GenericDatumReader<Object> reader = null;
	    JsonEncoder encoder = null;
	    ByteArrayOutputStream output = null;
	    try {
	        reader = new GenericDatumReader<Object>();
	        InputStream input = new ByteArrayInputStream(avro);
	        output = new ByteArrayOutputStream();
	        DataFileStream<Object> dataFileStream = new DataFileStream<Object>(input, reader);
			Schema schema = dataFileStream.getSchema();
	        DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
	        System.out.println("Schema :: " + schema.getName());
	        encoder = EncoderFactory.get().jsonEncoder(schema, output);
	        /*
	        for (Object datum : dataFileStream) {
	            writer.write(datum, encoder);
	        }*/
	        while(dataFileStream.hasNext()) {
	        	Object datum = dataFileStream.next();
	        	//System.out.println("datum : " + datum);
	        	writer.write(datum, encoder);
	        	break;
	        }
	        encoder.flush();
	        output.flush();
	        return new String(output.toByteArray());
	    } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
	        try { 
	        	((Closeable) reader).close(); 
	        	
	        } catch (Exception e) {}
	    }
	    return null;
	}
}
