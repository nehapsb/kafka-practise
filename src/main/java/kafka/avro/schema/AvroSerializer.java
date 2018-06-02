package kafka.avro.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class AvroSerializer {

	public static void main(String[] args) throws IOException {
		// File file = new File("users.avro");
		byte[] encoded = Files.readAllBytes(
				Paths.get("/Users/neha_pasbola/Documents/Workspace2/kafka-project/src/main/resources/users.avsc"));
		String str = new String(encoded, "UTF-8");
		System.out.println("str: " + str);
		Schema schema = new Schema.Parser().parse(
				new File("/Users/neha_pasbola/Documents/Workspace2/kafka-project/src/main/resources/users.avsc"));
		// Schema schema = new Schema.Parser().parse(file);

		GenericRecord recordUser1 = new GenericData.Record(schema);
		recordUser1.put("name", "User1");
		recordUser1.put("favorite_number", 2);

		GenericRecord recordUser2 = new GenericData.Record(schema);
		recordUser2.put("name", "User2");
		recordUser2.put("favorite_number", 7);
		recordUser2.put("favorite_color", "black");

		// Serialize recordUser1 and recordUser2 to disk
		File file = new File("users.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(recordUser1);
		dataFileWriter.append(recordUser2);
		dataFileWriter.close();

		// Deserialize users from disk
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
		String fileContent = new String(Files.readAllBytes(file.toPath()));
		System.out.println("AVRO File");
		System.out.println(fileContent);
		GenericRecord userRecord = null;
		while (dataFileReader.hasNext()) {
			userRecord = dataFileReader.next(userRecord);
			System.out.println(userRecord);

		}
		dataFileReader.close();
	}

}
