//Author:   Erik Abramczyk
//Created:  July 26, 2018
//Purpose:  Single thread that will churn through the parsedObjects queue, committing objects to the database

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.BlockingQueue;

import maverick_data.DatabaseInteraction;
import maverick_data.DatabaseType;
import org.json.JSONArray;
import org.json.JSONObject;

public class CommitThread implements Runnable
{
    private BlockingQueue<JSONObject> parsedObjects;

    private HashMap<String, String> tableNamesForJSONGroups;

    public CommitThread(BlockingQueue<JSONObject> parPointer)
    {
        parsedObjects = parPointer;

        tableNamesForJSONGroups = new HashMap<>();
        tableNamesForJSONGroups.put("identifiers", "device_identifiers");
        tableNamesForJSONGroups.put("product_codes", "device_product_codes");
        tableNamesForJSONGroups.put("customer_contacts", "device_customer_contacts");
        tableNamesForJSONGroups.put("gmdn_terms", "device_gmdn_terms");
        tableNamesForJSONGroups.put("device_sizes", "device_device_sizes");
        tableNamesForJSONGroups.put("storage", "device_storage");



    }

    public void run()
    {

        List<String> deviceInsertStatements = new LinkedList<>();
        List<String> otherInsertStatements = new LinkedList<>();
        DatabaseInteraction database;

        while(true)
        {
            try
            {
                if (parsedObjects.isEmpty() || deviceInsertStatements.size() > 10000)
                {
                    database = new DatabaseInteraction(DatabaseType.Devices);
                    System.out.println("Sending......");
                    //commit what sql statements we have now while parsers catch up
                    if (deviceInsertStatements.isEmpty() == false)
                    {
                        commitStatements(database, deviceInsertStatements, otherInsertStatements);
                        System.out.println("Committed records");
                        deviceInsertStatements.clear();
                        otherInsertStatements.clear();
                    }
                    database.closeConnection();
                }
                //take objects off the queue to make it not so large
                JSONObject object = parsedObjects.take();
                String deviceFDAID = object.getJSONArray("identifiers").getJSONObject(0).getString("id");
                Set<String> keys = object.keySet();

                List<FDADeviceProperty> deviceProperties = new LinkedList<>();

                for (String key : keys)
                {
                    Object result = object.get(key);

                    //parse object keys/values to commit values
                    if (result instanceof String)
                    {
                        //column and value get put into the insert statement
                        deviceProperties.add(new FDADeviceProperty(key, result));
                    }
                    else if (result instanceof JSONArray)
                    {
                        //all values in array get keys and values added to the insert statement
                        //cast the result to an array for property access
                        List<Object> arrayValues = ((JSONArray)result).toList();
                        List<FDADeviceProperty> arrayProps = new ArrayList<>();
                        String tableName = tableNamesForJSONGroups.get(key);
                        if (tableName == null)
                        {
                            //make sure key has been mapped to a coorisponding table
                            System.err.println("Can't find table's name for key in map");
                            continue;
                        }

                        for (Object val : arrayValues)
                        {
                            //ensure array object is a hash map as expected
                            if (val instanceof HashMap == false)
                            {
                                System.err.println("Invalid array value for key: " + key);
                                continue;
                            }
                            //cast object and get all keys
                            HashMap valMapCast = (HashMap)val;
                            Set<String> valKeys = valMapCast.keySet();
                            for (String valKey : valKeys)
                            {
                                //add this value from the map to the list for this JSON array
                                Object mappedValue = valMapCast.get(valKey);
                                //simple value of a string
                                if (mappedValue instanceof String)
                                {
                                    arrayProps.add(new FDADeviceProperty(valKey, valMapCast.get(valKey)));
                                }
                                if (mappedValue instanceof HashMap)
                                {
                                    //special hashmap value
                                    //two tables have hashmap values, storage and product codes, ensure it is one of these
                                    if (key.equals("product_codes") == false && key.equals("storage") == false)
                                    {
                                        System.err.println("Invalid hashmap subvalue");
                                    }
                                    HashMap castedMappedValue = (HashMap)mappedValue;
                                    Set<String> mappedValueKeys = castedMappedValue.keySet();

                                    //handle product_code's special case
                                    if (key.equals("product_codes"))
                                    {
                                        //product codes just needs the mapped values to get added to the array's properties
                                        for (String k : mappedValueKeys)
                                        {
                                            arrayProps.add(new FDADeviceProperty(k, castedMappedValue.get(k)));
                                        }
                                    }
                                    else
                                    {
                                        //handle storage's special case
                                        if (valKey.equals("high"))
                                        {
                                            //handle high value
                                            arrayProps.add(new FDADeviceProperty("high_value", castedMappedValue.get("value")));
                                            arrayProps.add(new FDADeviceProperty("high_unit", castedMappedValue.get("unit")));
                                        }
                                        else if (valKey.equals("low"))
                                        {
                                            //handle low value
                                            arrayProps.add(new FDADeviceProperty("low_value", castedMappedValue.get("value")));
                                            arrayProps.add(new FDADeviceProperty("low_unit", castedMappedValue.get("unit")));
                                        }
                                        else
                                        {
                                            System.err.println("Invalid subkey for storage key");
                                        }
                                    }
                                }

                            }
                            String arrayInsertStatement = fdaDevicePropertyQueryBuilder(tableName, deviceFDAID, arrayProps);
                            otherInsertStatements.add(arrayInsertStatement);
                            arrayProps.clear();
                        }
                    }
                    else if (result instanceof JSONObject)
                    {
                        List<FDADeviceProperty> objectProps = new ArrayList<>();
                        //all values of the object get keys and values added to the insert statement
                        //System.out.println("JSON Object");
                        if (key.equals("sterilization") == false)
                        {
                            //make sure JSON object is the sterilization object, which becomes a device property
                            System.err.println("Invalid JSON Child Object found");
                            continue;
                        }
                    }
                    else
                    {
                        //some odd object, throw error due to not knowing how to process.
                        System.err.println("Unknown object encountered, please restart commit");
                    }

                }

                deviceInsertStatements.add(fdaDevicePropertyQueryBuilder("devices", deviceFDAID, deviceProperties));
                deviceProperties.clear();
            }
            catch (InterruptedException IEE)
            {
                System.out.println("Commit Thread Interrupted");
            }
        }
    }

    private void commitStatements(DatabaseInteraction database, List<String> statements, List<String> statements2)
    {
        //get a prepared statement with the first device to commit
        PreparedStatement prep;

        if (statements.size() > 0)
        {
            prep = database.prepareStatement(statements.get(0));
        }
        else
        {
            prep = database.prepareStatement(statements2.get(0));
        }

        for (String s : statements)
        {
            //add each sql statement as a batch to the prepared statement
            try
            {
                prep.addBatch(s);
            }
            catch (SQLException sqlex)
            {
                System.err.println("Invalid SQL Statement in Commit compilation: \n" + sqlex.getMessage());
            }
        }
        for (String s : statements2)
        {
            //add each sql statement as a batch to the prepared statement
            try
            {
                prep.addBatch(s);
            }
            catch (SQLException sqlex)
            {
                System.err.println("Invalid SQL Statement in Commit compilation: \n" + sqlex.getMessage());
            }
        }
        database.nonQueryBatch(prep);
    }

    private String fdaDevicePropertyQueryBuilder(String tableName, String fdaId, List<FDADeviceProperty> props){
        //Method to build an insert nonquery string from properties parsed from a data object
        String writeDeviceColumnsSql = "INSERT INTO " + tableName + "(fda_id";
        String writeDeviceValuesSql = ") VALUES ('" + fdaId + "'";
        for (FDADeviceProperty thisProperty : props) {
            //Loop through each property and add it to the nonquery
            //fix misspellings in data
            if (thisProperty.getColumnName().equals("ext"))
            {
                thisProperty.colName = "text";
            }
            writeDeviceColumnsSql += ", `" + thisProperty.getColumnName() + "`";
            writeDeviceValuesSql += ", ";
            Object propertyValue = thisProperty.getValue();
            if (propertyValue == null)
            {
                writeDeviceValuesSql += "NULL";
            }
            else if(propertyValue.getClass() == String.class){
                //If the value of the property is a string, it needs to have the single quotes
                String propValString = propertyValue.toString();
                propValString = propValString.replace("'", "''");
                writeDeviceValuesSql += "'" + propValString + "'";
            }
            else if(propertyValue.getClass() == Boolean.class){
                //If the value of the property is a boolean, it needs to be converted to a MySQL tinyint
                if((Boolean)propertyValue){
                    writeDeviceValuesSql += "1";
                } else {
                    writeDeviceValuesSql += "0";
                }
            } else if (propertyValue.getClass() == int.class || propertyValue.getClass() == Integer.class) {
                //If it is an integer it can just be added on no problem
                writeDeviceValuesSql += propertyValue;
            }
        }
        //Combine and close the query and return it
        return writeDeviceColumnsSql + writeDeviceValuesSql + ")";
    }


}
