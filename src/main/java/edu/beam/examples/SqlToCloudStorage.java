package edu.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;


public class SqlToCloudStorage {
    public interface SqlToCloudStorageOptions extends PipelineOptions {
        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);

        @Description("URL of the DB")
        @Validation.Required
        String getUrl();

        void setUrl(String value);

        @Description("DB user")
        @Validation.Required
        String getUser();

        void setUser(String value);

        @Description("DB password")
        @Validation.Required
        String getPassword();

        void setPassword(String value);
    }

    public static class CarFilter implements SerializableFunction<String, Boolean> {
        @Override
        public Boolean apply(String input) {
            assert input != null;
            return !input.startsWith("R");
        }
    }

    public static class PriceManager extends DoFn<String, String> {
        @ProcessElement
        public void
        processElement(ProcessContext c) {
            String[] car = Objects.requireNonNull(c.element()).split(" ");
            int mileage = Integer.parseInt(car[1]);
            if (mileage > 100) {
                double price = Double.parseDouble(car[2]) * 0.9;
                c.output(car[0] + " " + car[1] + " " + price);
            }
        }
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        SqlToCloudStorageOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(SqlToCloudStorageOptions.class);
        Pipeline p = Pipeline.create(options);

        List<String> cars = DB.getCars(options);

        p
                .apply(Create.of(cars)).setCoder(StringUtf8Coder.of())
                .apply(Filter.greaterThan("200"))
                .apply(ParDo.of(new PriceManager()))
                .apply("WriteUsers", TextIO.write().to(options.getOutput()));
        p.run().waitUntilFinish();
    }
}


class DB {
    public static List<String> getCars(SqlToCloudStorage.SqlToCloudStorageOptions options) throws SQLException, ClassNotFoundException {
        List<String> users = new ArrayList<>();
        Connection connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(options.getUrl(), options.getUser(), options.getPassword());
            Statement statement = connection.createStatement();

            ResultSet result1 = statement.executeQuery("select * from \"car\"");
            while (result1.next()) {
                users.add(result1.getString("marka") + " " +
                        result1.getInt("mileage") + " " +
                        result1.getDouble("price"));
            }
        } catch (Exception ex) {
            Logger.getLogger(SqlToCloudStorage.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(SqlToCloudStorage.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return users;
    }
}
