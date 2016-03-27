package com.yan2.spark;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Main class to load and process WhatsApp messages with Spark.
 * 
 * @author Haiyan
 *
 */
public class WhatsAppSparkProcessor implements Serializable {

    /**
     * Serial ID.
     */
    private static final long serialVersionUID = 1088351933227904376L;

    static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy, HH:mm");

    /**
     * Message pattern of each line in WhatsApp message that contains the date,
     * user and message body. If a line doesn't match the pattern it is
     * considered as a new line of the previous message body.
     */
    static final Pattern MESSAGE_PATTERN = Pattern.compile("^(\\d{2}/\\d{2}/\\d{4}, \\d{2}:\\d{2}) - ([^:]*): (.*)$");

    /**
     * Separator between a multi-line message body.
     */
    static final String MESSAGE_LINE_SEPARATOR = "\\n";

    /**
     * Main method.
     * 
     * @param args
     *            First argument must contain the absolute path to the file to
     *            parse.
     */
    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Must have one and only one argument that is the absolute path to the file to parse.");
        } else {
            WhatsAppSparkProcessor whatsApp = new WhatsAppSparkProcessor();

            whatsApp.process(args[0]);
        }
    }

    /**
     * Load WhatsApp message to Spark DataFrame.
     * 
     * @param inputFile
     *            Absolute path to the file to parse.
     */
    public void process(final String inputFile) {

        SparkConf conf = new SparkConf().setAppName("SimpleApp").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        List<WhatsAppMessage> messageList = new ArrayList<>();

        try (Stream<String> inputLinesStream = Files.lines(FileSystems.getDefault().getPath(inputFile))) {

            Iterator<String> it = inputLinesStream.iterator();
            while (it.hasNext()) {
                String line = it.next();
                Matcher m = MESSAGE_PATTERN.matcher(line);

                if (m.matches()) {
                    WhatsAppMessage message = createWhatsAppMessage(m.group(1), m.group(2), m.group(3));
                    messageList.add(message);
                } else {
                    if (!messageList.isEmpty()) {
                        WhatsAppMessage prevMessage = messageList.get(messageList.size() - 1);
                        prevMessage.setMessage(prevMessage.getMessage() + MESSAGE_LINE_SEPARATOR + line);
                        prevMessage.setLineCount(prevMessage.getLineCount() + 1);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create RDD and convert it to DataFrame
        JavaRDD<WhatsAppMessage> whatsAppMessage = sc.parallelize(messageList);
        DataFrame schemaWhatsAppMessage = sqlContext.createDataFrame(whatsAppMessage, WhatsAppMessage.class);
        schemaWhatsAppMessage.printSchema();
        schemaWhatsAppMessage.registerTempTable("whatsAppMessage");

        whatsAppMessage.foreach(System.out::println);

        // Run SQL on the registered as tables
        DataFrame users = sqlContext.sql("SELECT DISTINCT user FROM whatsAppMessage");
        List<String> userNames = users.javaRDD().map(Row::toString).collect();

        userNames.forEach(System.out::println);

    }

    /**
     * Create WhatsAppMessage.
     * 
     * @param date
     *            String representation of the message date in format
     *            <code>"dd/MM/yyyy, HH:mm"</code> (e.g. 11/03/2015, 20:23).
     * @param user
     *            User who sent the message.
     * @param message
     *            Content of the message.
     * @return WhatsAppMessage.
     */
    WhatsAppMessage createWhatsAppMessage(final String date, final String user, final String message) {
        WhatsAppMessage whatsAppMessage = new WhatsAppMessage();
        whatsAppMessage.setLineCount(1);

        LocalDateTime dateTime = LocalDateTime.parse(date, formatter);
        Timestamp t = Timestamp.valueOf(dateTime);
        whatsAppMessage.setDateTime(t);

        whatsAppMessage.setUser(user);
        whatsAppMessage.setMessage(message);

        return whatsAppMessage;
    }
}
