package com.roy.avroexample;

import example.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroWriter {
    public void writeToFile() {
        User user1 = new User("Roy", 9, "Violet");

        // Alternate constructor
        User user2 = new User("Ren", 8, "Red");

        // Constructor via builder
        // Note that builders require all fields even if they're null
        User user3 = User.newBuilder()
                .setName("Charles")
                .setFavoriteColor(null)
                .setFavoriteNumber(4)
                .build();

        // Serialize all of our users to disk (write it to a file)
        // The DatumWriter converts Java objects into an in-memory serialized format.
        // The SpecificDatumWriter is used with generated classes.
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        // The DataFileWriter writes the serialized records and the schema.
        // Recall that the try with resources block will create the DataFileWriter
        // and close it automatically without having to call close()
        File avroFile = new File("users.avro");
        try (DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter)) {
            // Specify the file we want to write to
            dataFileWriter.create(user1.getSchema(), avroFile);
            // These method calls will write our users to this file
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Deserialize the data we just created (read our users from the avro file)
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);

        try (DataFileReader<User> dataFileReader = new DataFileReader<User>(avroFile, userDatumReader)) {
            User user = null;
            while (dataFileReader.hasNext()) {
                // Reuse user object by passing it to next()
                // We can save allocating and garbage collecting many objects this way
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
