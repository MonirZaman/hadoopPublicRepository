import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;


public class AvroFileReader {

	/**
	 * main method reads an avro file from disc
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		File f=new File("/home/training/workspace/avroPlay/records.avro");
		
		//Note: we don't need to pass schema as it is stored in the avro file
		DatumReader<GenericRecord> r = new GenericDatumReader<GenericRecord>();
		DataFileReader<GenericRecord> dfReader = new DataFileReader<GenericRecord>(f, r);
		
		
		//DataFileReader also implements Iterator interface 
		//GenericRecord uses schema to determine the type of each field
		for(GenericRecord rec:dfReader){
			//printing all the fields in rec
			System.out.println(rec);
		}


	}

}
