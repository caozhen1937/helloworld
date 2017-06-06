package tracking;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by yuchaoma on 19/04/2017.
 */
public class Utils {
    public static RowResult getUser(GenericRecord eventRecord, KuduTable table, KuduClient client){
        KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);
        ColumnSchema appkeyColumn = new ColumnSchema.ColumnSchemaBuilder("appkey",  Type.getTypeForDataType(Common.DataType.STRING)).build();
        KuduPredicate appkeyPredicate = KuduPredicate.newComparisonPredicate(appkeyColumn, KuduPredicate.ComparisonOp.EQUAL, eventRecord.get("appKey").toString());
        ColumnSchema anonymousIdColumn = new ColumnSchema.ColumnSchemaBuilder("anonymousid",  Type.getTypeForDataType(Common.DataType.STRING)).build();
        KuduPredicate anonymousIdPredicate = KuduPredicate.newComparisonPredicate(anonymousIdColumn, KuduPredicate.ComparisonOp.EQUAL, eventRecord.get("anonymousId").toString());
        builder.addPredicate(appkeyPredicate);
        builder.addPredicate(anonymousIdPredicate);
        KuduScanner scanner = builder.build();
        RowResultIterator results = null;
        try {
            results = scanner.nextRows();
        } catch (KuduException e) {
            e.printStackTrace();
        }
        if(results != null && results.hasNext()){ return results.next(); }
        return null;
    }

    public static Long toSeconds(String timeStr) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        timeStr = timeStr.substring(0,19).replace("T", " ");
        Date date = formatter.parse(timeStr);
        return date.getTime()/1000;
    }


    public static void insertToEventPropertyNames(String appKey, String event, String propertiesStr, KuduTable table,  KuduSession session) throws IOException {
        if(propertiesStr != null){
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            JsonNode properties = null;
            properties = mapper.readTree(propertiesStr);
            for (Iterator<Map.Entry<String, JsonNode>> it = properties.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addString("appkey", appKey);
                row.addString("event", event);
                row.addString("property_name", entry.getKey());
                session.apply(insert);
            }
        }
    }

    public static GenericRecord fillAvroFromJson(GenericRecord record, JsonNode source, HashMap<String, Object[]> fieldMappings){
        for(Map.Entry<String, Object[]> entry: fieldMappings.entrySet()){
            String key = entry.getKey();
            String dataType = (String) entry.getValue()[0];
            String[] fieldNames = (String[]) entry.getValue()[1];

            JsonNode node;
            node = source;
            for(String fieldName: fieldNames){
                if(node.get(fieldName) != null){
                    node = node.get(fieldName);
                }else{
                    node = null;
                    break;
                }
            }
            if(node != null){
                if(dataType.equals("String")){
                    if(node.isValueNode()){
                        record.put(key, node.asText());
                    }else{
                        record.put(key, node.toString());
                    }
                }else if (dataType.equals("Long")){
                    record.put(key, node.asLong());
                }else if (dataType.equals("Double")){
                    record.put(key, node.asDouble());
                }else if (dataType.equals("Int")){
                    record.put(key, node.asInt());
                }else if (dataType.equals("Boolean")){
                    record.put(key, node.asBoolean());
                }
            }
        }
        return record;
    }

    public static PartialRow fillRowFromAvro(PartialRow row, GenericRecord record, HashMap<String, String[]> fieldMappings){
        for(Map.Entry<String, String[]> entry: fieldMappings.entrySet()){
            String key = entry.getKey();
            String dataType =  entry.getValue()[0];
            String fieldName =  entry.getValue()[1];
            Object node = record.get(fieldName);
            if(node != null){
                if(dataType.equals("String")){
                    row.addString(key, node.toString());
                }else if (dataType.equals("Long")){
                    row.addLong(key, (long) node);
                }else if (dataType.equals("Double")){
                    row.addDouble(key, (double) node);
                }else if (dataType.equals("Int")){
                    row.addInt(key, (int) node);
                }else if (dataType.equals("Boolean")){
                    row.addBoolean(key, (boolean) node);
                }
            }
        }
        return row;
    }
}
