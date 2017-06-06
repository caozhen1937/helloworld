
import im.GroupOperationConverter;
import im.ReceiveMessageConverter;
import im.SaveImAttachment;
import im.*;
import org.apache.commons.cli.*;
import tracking.SourceToEvents;
import tracking.SourceToUsers;
import tracking.WriteEventsToKudu;
import tracking.WriteUsersToKudu;

import java.io.IOException;
import java.util.Properties;

public class Main {


    public static void main(String[] args) throws Exception {
        Options options = new Options();
        Option task  = Option.builder("task").argName( "task name" )
                .hasArg().desc( "Specify a task type to run")
                .required(true).build();
        Option property  = Option.builder("D").argName( "property=value" )
                .hasArgs().valueSeparator()
                .desc("use value for given property").build();
        Option config = Option.builder("config").argName("path").hasArg()
                .desc("specify config file").build();
        options.addOption(task);
        options.addOption(property);
        options.addOption(config);

        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            String taskName = line.getOptionValue("task");
            Properties properties = line.getOptionProperties("D");
            if(taskName.equals("im_msg")){
                convertImMessage(properties);
            }else if( taskName.equals("im_group_op")){
                convertImGroupOperation(properties);
            }else if(taskName.equals("stream_to_events")){
                streamToEvents(properties);
            }else if(taskName.equals("events_to_kudu")){
                eventsToKudu(properties);
            }else if(taskName.equals("stream_to_users")){
                streamToUsers(properties);
            }else if(taskName.equals("users_to_kudu")){
                usersToKudu(properties);
            }else if(taskName.equals("save_im_attach")){
                saveImAttach(properties);
            }else if(taskName.equals("first_test")){
                firstMessageConverter(properties);
            }else{
                throw new RuntimeException("unknown task name");
            }

        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }
    }



    public static void firstMessageConverter(Properties properties) throws Exception {

        FirstMessageConverter firstMessageConverter = new FirstMessageConverter(properties);
        firstMessageConverter.run();
    }
    public static void convertImMessage(Properties properties) throws IOException {

        ReceiveMessageConverter converter = new ReceiveMessageConverter(properties);
        converter.run();
    }

    public static  void convertImGroupOperation(Properties properties) throws IOException{
        GroupOperationConverter converter = new GroupOperationConverter(properties);
        converter.run();
    }

    public static  void streamToEvents(Properties properties) throws IOException{
        SourceToEvents task = new SourceToEvents(properties);
        task.run();
    }

    public static  void eventsToKudu(Properties properties) throws IOException{
        WriteEventsToKudu task = new WriteEventsToKudu(properties);
        task.run();
    }

    public static  void streamToUsers(Properties properties) throws IOException{
        SourceToUsers task = new SourceToUsers(properties);
        task.run();
    }

    public static  void usersToKudu(Properties properties) throws IOException{
        WriteUsersToKudu task = new WriteUsersToKudu(properties);
        task.run();
    }

    private static void saveImAttach(Properties properties) throws IOException {
        SaveImAttachment task = new SaveImAttachment(properties);
        task.run();
    }

}
