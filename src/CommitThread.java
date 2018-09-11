//Author:   Erik Abramczyk
//Created:  July 26, 2018
//Purpose:  Single thread that will churn through the parsedObjects queue, committing objects to the database

import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.BlockingQueue;

import maverick_data.DatabaseInteraction;
import maverick_data.Config;
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

        while(true)
        {
            try
            {
                if (parsedObjects.isEmpty())
                {
                    //commit what sql statements we have now while parsers catch up
                    if (deviceInsertStatements.isEmpty() == false)
                    {
                        //has device inserts to perform
                        String accumulatedSQLStatements = "";
                        for (String s : deviceInsertStatements)
                        {
                            accumulatedSQLStatements += s + ";\n";
                        }
                        System.out.println("fuck");
                    }
                    if (otherInsertStatements.isEmpty() == false)
                    {
                        //has other inserts to perform
                    }
                    //System.out.println("fuck");
                }
                //take objects off the queue to make it not so large
                JSONObject object = parsedObjects.take();
                DatabaseInteraction database = new DatabaseInteraction(Config.host, Config.port, Config.user, Config.pass, Config.databaseName);
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
                            }
            catch (InterruptedException IEE)
            {
                System.out.println("Commit Thread Interrupted");
            }
        }
    }

    private void writeDevice(JSONObject readObject, DatabaseInteraction database){
        //Write the device records and all of its supporting data object records to the database
        //Every device has an FDA id in its identifier child object under the key id, fetch this as this will be the device database key
        String fdaId = readObject.getJSONArray("identifiers").getJSONObject(0).getString("id");
        //Get the properties of the device from the JSON object
        List<FDADeviceProperty> deviceProperties = getDeviceProperties(readObject);
        //Build the query to write the device record based on the properties parsed from the JSON file
        String deviceInsertSql = fdaDevicePropertyQueryBuilder("fda_data_devices", fdaId ,deviceProperties);
        //Write the built query to the database
        PreparedStatement deviceInsertQuery = database.prepareStatement(deviceInsertSql);
        database.nonQuery(deviceInsertQuery);
        if(readObject.has("customer_contacts")){
            //Device has customer contacts child object, write these to the database
            writeDeviceCustomerContacts(readObject, fdaId, database);
        }
        if(readObject.has("device_sizes")){
            //Device has sizes, write these to the database
            writeDeviceSizes(readObject, fdaId, database);
        }
        if(readObject.has("gmdn_terms")){
            //Device has gmdn term objects, write these to the database
            writeDeviceGmdnTerms(readObject, fdaId, database);
        }
        if(readObject.has("identifiers")){
            //Device has identifiers, write these to the database
            writeDeviceIdentifiers(readObject, fdaId, database);
        }
        if(readObject.has("premarket_submissions")){
            //Device has premarket submissions, write these to the database
            writeDevicePremarketSubmissions(readObject, fdaId, database);
        }
        if(readObject.has("product_codes")){
            //Device has product codes, write these to the database
            writeDeviceProductCodes(readObject, fdaId, database);
        }
        if(readObject.has("storage")){
            //Device has storage details, write these to the database
            writeDeviceStorage(readObject, fdaId, database);
        }
    }

    private List<FDADeviceProperty> getDeviceProperties(JSONObject readObject){
        //Parse a device record object
        List<FDADeviceProperty> props = new ArrayList<>();
        //Parse the device record data attributes
        if(readObject.has("brand_name")){ props.add(new FDADeviceProperty("brand_name", readObject.getString("brand_name"))); }
        if(readObject.has("catalog_number")){ props.add(new FDADeviceProperty("catalog_number", readObject.getString("catalog_number"))); }
        if(readObject.has("commercial_distribution_end_date")){ props.add(new FDADeviceProperty("commercial_distribution_end_date", readObject.getString("commercial_distribution_end_date"))); }
        if(readObject.has("commercial_distribution_status")){ props.add(new FDADeviceProperty("commercial_distribution_status", readObject.getString("commercial_distribution_status"))); }
        if(readObject.has("company_name")){ props.add(new FDADeviceProperty("company_name", readObject.getString("company_name"))); }
        if(readObject.has("device_count_in_base_package")){ props.add(new FDADeviceProperty("device_count_in_base_package", readObject.getInt("device_count_in_base_package"))); }
        if(readObject.has("device_description")){ props.add(new FDADeviceProperty("device_description", readObject.getString("device_description"))); }
        if(readObject.has("has_donation_id_number")){ props.add(new FDADeviceProperty("has_donation_id_number", readObject.getBoolean("has_donation_id_number"))); }
        if(readObject.has("has_expiration_date")){ props.add(new FDADeviceProperty("has_expiration_date", readObject.getBoolean("has_expiration_date"))); }
        if(readObject.has("has_lot_or_batch_number")){ props.add(new FDADeviceProperty("has_lot_or_batch_number", readObject.getBoolean("has_lot_or_batch_number"))); }
        if(readObject.has("has_manufacturing_date")){ props.add(new FDADeviceProperty("has_manufacturing_date", readObject.getBoolean("has_manufacturing_date"))); }
        if(readObject.has("has_serial_number")){ props.add(new FDADeviceProperty("has_serial_number", readObject.getBoolean("has_serial_number"))); }
        if(readObject.has("is_combination_product")){ props.add(new FDADeviceProperty("is_combination_product", readObject.getBoolean("is_combination_product"))); }
        if(readObject.has("is_direct_marking_exempt")){ props.add(new FDADeviceProperty("is_direct_marking_exempt", readObject.getBoolean("is_direct_marking_exempt"))); }
        if(readObject.has("is_hct_p")){ props.add(new FDADeviceProperty("is_hct_p", readObject.getBoolean("is_hct_p"))); }
        if(readObject.has("is_kit")){ props.add(new FDADeviceProperty("is_kit", readObject.getBoolean("is_kit"))); }
        if(readObject.has("is_labeled_as_no_nrl")){ props.add(new FDADeviceProperty("is_labeled_as_no_nrl", readObject.getBoolean("is_labeled_as_no_nrl"))); }
        if(readObject.has("is_labeled_as_nrl")){ props.add(new FDADeviceProperty("is_labeled_as_nrl", readObject.getBoolean("is_labeled_as_nrl"))); }
        if(readObject.has("is_otc")){ props.add(new FDADeviceProperty("is_otc", readObject.getBoolean("is_otc"))); }
        if(readObject.has("is_pm_exempt")){ props.add(new FDADeviceProperty("is_pm_exempt", readObject.getBoolean("is_pm_exempt"))); }
        if(readObject.has("is_rx")){ props.add(new FDADeviceProperty("is_rx", readObject.getBoolean("is_rx"))); }
        if(readObject.has("is_single_use")){ props.add(new FDADeviceProperty("is_single_use", readObject.getBoolean("is_single_use"))); }
        if(readObject.has("labeler_duns_number")){ props.add(new FDADeviceProperty("labeler_duns_number", readObject.getString("labeler_duns_number"))); }
        if(readObject.has("mri_safety")){ props.add(new FDADeviceProperty("mri_safety", readObject.getString("mri_safety"))); }
        if(readObject.has("public_version_date")){ props.add(new FDADeviceProperty("public_version_date", readObject.getString("public_version_date"))); }
        if(readObject.has("public_version_number")){ props.add(new FDADeviceProperty("public_version_number", readObject.getString("public_version_number"))); }
        if(readObject.has("public_version_status")){ props.add(new FDADeviceProperty("public_version_status", readObject.getString("public_version_status"))); }
        if(readObject.has("publish_date")){ props.add(new FDADeviceProperty("publish_date", readObject.getString("publish_date"))); }
        if(readObject.has("record_key")){ props.add(new FDADeviceProperty("record_key", readObject.getString("record_key"))); }
        if(readObject.has("record_status")){ props.add(new FDADeviceProperty("record_status", readObject.getString("record_status"))); }
        if(readObject.has("is_sterile")){ props.add(new FDADeviceProperty("is_sterile", readObject.getBoolean("is_sterile"))); }
        if(readObject.has("is_sterilization_prior_use")){ props.add(new FDADeviceProperty("is_sterilization_prior_use", readObject.getBoolean("is_sterilization_prior_use"))); }
        if(readObject.has("sterilization_methods")){ props.add(new FDADeviceProperty("sterilization_methods", readObject.getString("sterilization_methods"))); }
        if(readObject.has("version_or_model_number")){ props.add(new FDADeviceProperty("version_or_model_number", readObject.getString("version_or_model_number"))); }
        //Parse the device record properties that are nested within the product codes child object if it exists
        if(readObject.has("product_codes")){
            JSONArray productCodes = readObject.getJSONArray("product_codes");
            if(productCodes.length() != 0) {
                if (productCodes.getJSONObject(0).has("openfda")) {
                    JSONObject openFda = productCodes.getJSONObject(0).getJSONObject("openfda");
                    if (openFda.has("device_class")) {
                        props.add(new FDADeviceProperty("device_class", openFda.getString("device_class")));
                    }
                    if (openFda.has("device_name")) {
                        props.add(new FDADeviceProperty("device_name", openFda.getString("device_name")));
                    }
                    if (openFda.has("fei_number")) {
                        props.add(new FDADeviceProperty("fei_number", openFda.getString("fei_number")));
                    }
                    if (openFda.has("medical_specialty_description")) {
                        props.add(new FDADeviceProperty("medical_specialty_description", openFda.getString("medical_specialty_description")));
                    }
                    if (openFda.has("regulation_number")) {
                        props.add(new FDADeviceProperty("regulation_number", openFda.getString("regulation_number")));
                    }
                }
            }
        }
        if(readObject.has("device_name")){ props.add(new FDADeviceProperty("device_name", readObject.getString("device_name"))); }
        if(readObject.has("fei_number")){ props.add(new FDADeviceProperty("fei_number", readObject.getString("fei_number"))); }
        if(readObject.has("medical_specialty_description")){ props.add(new FDADeviceProperty("medical_specialty_description", readObject.getString("medical_specialty_description"))); }
        if(readObject.has("regulation_number")){ props.add(new FDADeviceProperty("regulation_number", readObject.getString("regulation_number"))); }
        return props;
    }

    private void writeDeviceCustomerContacts(JSONObject readObject, String fdaId, DatabaseInteraction database){
        //Parse a customer contact object, build its insert nonquery, and write to database
        //Get the array containing the device's customer contacts
        JSONArray customerContactsArray = readObject.getJSONArray("customer_contacts");
        for (int i = 0; i < customerContactsArray.length(); i++){
            //Loop through all of the customer contacts within the array, parse, and write all of them to the database
            JSONObject thisCustomerContact = customerContactsArray.getJSONObject(i);
            List<FDADeviceProperty> props = new ArrayList<>();
            //Parse the data attributes of the customer contact object
            if(thisCustomerContact.has("phone")){ props.add(new FDADeviceProperty("phone", thisCustomerContact.getString("phone"))); }
            if(thisCustomerContact.has("email")){ props.add(new FDADeviceProperty("email", thisCustomerContact.getString("email"))); }
            //Write the customer contact object to the database
            String thisCustomerContactInsertSql = fdaDevicePropertyQueryBuilder("fda_data_device_customer_contacts", fdaId, props);
            PreparedStatement customerContactInsertQuery = database.prepareStatement(thisCustomerContactInsertSql);
            database.nonQuery(customerContactInsertQuery);
        }
    }

    private void writeDeviceSizes(JSONObject readObject, String fdaId, DatabaseInteraction database){
        //Parse a device size object, build its insert nonquery, and write to the database
        //Get the array containing the device's size objects
        JSONArray deviceSizesArray = readObject.getJSONArray("device_sizes");
        for(int i = 0; i < deviceSizesArray.length(); i++){
            //Loop through all of the device sizes, parse, and write all of them to the database
            JSONObject thisDeviceSize = deviceSizesArray.getJSONObject(i);
            List<FDADeviceProperty> props = new ArrayList<>();
            //Parse the data attributes of the device size object
            if(thisDeviceSize.has("text")){ props.add(new FDADeviceProperty("text", thisDeviceSize.getString("text"), "size_text")); }
            if(thisDeviceSize.has("type")){ props.add(new FDADeviceProperty("type", thisDeviceSize.getString("type"), "size_type")); }
            if(thisDeviceSize.has("value")){ props.add(new FDADeviceProperty("value", thisDeviceSize.getString("value"), "size_value")); }
            if(thisDeviceSize.has("unit")) { props.add(new FDADeviceProperty("unit", thisDeviceSize.getString("unit"))); }
            //Write the device size object to the database
            String thisDeviceSizeInsertSql = fdaDevicePropertyQueryBuilder("fda_data_device_device_sizes", fdaId, props);
            PreparedStatement deviceSizeInsertQuery = database.prepareStatement(thisDeviceSizeInsertSql);
            database.nonQuery(deviceSizeInsertQuery);
        }
    }

    private void writeDeviceGmdnTerms(JSONObject readObject, String fdaId, DatabaseInteraction database){
        //Parse a device GMDN terms object, build its insert nonquery, and write to the database
        JSONArray deviceGmdnTermsArray = readObject.getJSONArray("gmdn_terms");
        for(int i = 0; i < deviceGmdnTermsArray.length(); i++){
            //Loop through all of the gmdn term obejects, parse, and write them to the database
            JSONObject thisGmdnTerm = deviceGmdnTermsArray.getJSONObject(i);
            List<FDADeviceProperty> props = new ArrayList<>();
            //Parse the data attributes of the device gmdn terms object
            if(thisGmdnTerm.has("name")){ props.add(new FDADeviceProperty("name", thisGmdnTerm.getString("name"))); }
            if(thisGmdnTerm.has("definition")){ props.add(new FDADeviceProperty("definition", thisGmdnTerm.getString("definition"))); }
            //Write the device gmdn term to the database
            String thisGmdnTermsInsertSql = fdaDevicePropertyQueryBuilder("fda_data_device_gmdn_terms", fdaId, props);
            PreparedStatement gmdnTermsInsertQuery = database.prepareStatement(thisGmdnTermsInsertSql);
            database.nonQuery(gmdnTermsInsertQuery);
        }
    }

    private void writeDeviceIdentifiers(JSONObject readObject, String fdaId, DatabaseInteraction database){
        //Parse a device identifier object, build its insert nonquery, and write to the database
        JSONArray deviceIdentifiers = readObject.getJSONArray("identifiers");
        for(int i = 0; i < deviceIdentifiers.length(); i++){
            //Loop through all of the device identifier objects, parse, and write them all to the database
            JSONObject thisDeviceIdentifier = deviceIdentifiers.getJSONObject(i);
            List<FDADeviceProperty> props = new ArrayList<>();
            //Parse the data attributes of the device identifier object
            if(thisDeviceIdentifier.has("id")){ props.add(new FDADeviceProperty("id", thisDeviceIdentifier.getString("id"), "identifier_id")); }
            if(thisDeviceIdentifier.has("issuing_agency")){ props.add(new FDADeviceProperty("issuing_agency", thisDeviceIdentifier.getString("issuing_agency"))); }
            if(thisDeviceIdentifier.has("package_discontinue_date")){ props.add(new FDADeviceProperty("package_discontinue_date", thisDeviceIdentifier.getString("package_discontinue_date"))); }
            if(thisDeviceIdentifier.has("package_status")){ props.add(new FDADeviceProperty("package_status", thisDeviceIdentifier.getString("package_status"))); }
            if(thisDeviceIdentifier.has("package_type")){ props.add(new FDADeviceProperty("package_type", thisDeviceIdentifier.getString("package_type"))); }
            if(thisDeviceIdentifier.has("quantity_per_package")){ props.add(new FDADeviceProperty("quantity_per_package", thisDeviceIdentifier.getInt("quantity_per_package"))); }
            if(thisDeviceIdentifier.has("type")){ props.add(new FDADeviceProperty("type", thisDeviceIdentifier.getString("type"), "identifier_type")); }
            if(thisDeviceIdentifier.has("unit_of_use_id")){ props.add(new FDADeviceProperty("unit_of_use_id", thisDeviceIdentifier.getString("unit_of_use_id"))); }
            //Write the device identifier to the database
            String thisDeviceIdentifierInsertSql = fdaDevicePropertyQueryBuilder("fda_data_device_identifiers", fdaId, props);
            PreparedStatement deviceIdentifierQuery = database.prepareStatement(thisDeviceIdentifierInsertSql);
            database.nonQuery(deviceIdentifierQuery);
        }
    }

    private void writeDevicePremarketSubmissions(JSONObject readObject, String fdaId, DatabaseInteraction database){
        //Parse a device premarket submission object, build its insert nonquery, and write to the database
        JSONArray devicePremarketSubmissions = readObject.getJSONArray("premarket_submissions");
        for(int i = 0; i < devicePremarketSubmissions.length(); i++){
            //Loop through all of the device premarket submission objects, parse, and write them all to the database
            JSONObject thisPremarketSubmission = devicePremarketSubmissions.getJSONObject(i);
            List<FDADeviceProperty> props = new ArrayList<>();
            //Parse the data attributes of the device premarket submission object
            if(thisPremarketSubmission.has("submission_number")){ props.add(new FDADeviceProperty("submission_number", thisPremarketSubmission.getString("submission_number"))); }
            if(thisPremarketSubmission.has("supplement_number")){ props.add(new FDADeviceProperty("supplement_number", thisPremarketSubmission.getString("supplement_number"))); }
            if(thisPremarketSubmission.has("submission_type")){ props.add(new FDADeviceProperty("submission_type", thisPremarketSubmission.getString("submission_type"))); }
            //Write the premarket submission object to the database
            String thisPremarketSubmissionSql = fdaDevicePropertyQueryBuilder("fda_data_device_premarket_submissions", fdaId, props);
            PreparedStatement premarketSubmissionQuery = database.prepareStatement(thisPremarketSubmissionSql);
            database.nonQuery(premarketSubmissionQuery);
        }
    }

    private void writeDeviceProductCodes(JSONObject readObject, String fdaId, DatabaseInteraction database){
        //Parse a device product code object, build its insert nonquery, and write to the database
        JSONArray deviceProductCodes = readObject.getJSONArray("product_codes");
        for(int i = 0; i < deviceProductCodes.length(); i++){
            //Loop through all of the device product codes, parse, and write them all to the database
            JSONObject thisProductCode = deviceProductCodes.getJSONObject(i);
            List<FDADeviceProperty> props = new ArrayList<>();
            //Parse the data attributes of the device product code
            if(thisProductCode.has("code")){ props.add(new FDADeviceProperty("code", thisProductCode.getString("code"))); }
            if(thisProductCode.has("name")){ props.add(new FDADeviceProperty("name", thisProductCode.getString("name"))); }
            //Write the product code object to the database
            String thisProductCodeInsertSql = fdaDevicePropertyQueryBuilder("fda_data_device_product_codes", fdaId, props);
            PreparedStatement productCodeQuery = database.prepareStatement(thisProductCodeInsertSql);
            database.nonQuery(productCodeQuery);
        }
    }

    private void writeDeviceStorage(JSONObject readObject, String fdaId, DatabaseInteraction database){
        //Parse a device storage record, build its insert nonquery, and write to the database
        JSONArray deviceStorage = readObject.getJSONArray("storage");
        for(int i = 0; i < deviceStorage.length(); i++){
            //Loop through all of the device storage objects, parse, and write them all to the database
            JSONObject thisStorage = deviceStorage.getJSONObject(i);
            List<FDADeviceProperty> props = new ArrayList<>();
            if(thisStorage.has("high")){
                //Get the high value properties if the object contains a high listing
                JSONObject storageHigh = thisStorage.getJSONObject("high");
                if(storageHigh.has("value")){ props.add(new FDADeviceProperty("value", storageHigh.getString("value"), "high_value")); }
                if(storageHigh.has("unit")){ props.add(new FDADeviceProperty("unit", storageHigh.getString("unit"), "high_unit")); }
            }
            if(thisStorage.has("low")){
                //Get the low value properties if the object contains a low listing
                JSONObject storageLow = thisStorage.getJSONObject("low");
                if(storageLow.has("value")){ props.add(new FDADeviceProperty("value", storageLow.getString("value"), "low_value")); }
                if(storageLow.has("unit")){ props.add(new FDADeviceProperty("unit", storageLow.getString("unit"), "low_unit")); }
            }
            if(thisStorage.has("special_conditions")){ props.add(new FDADeviceProperty("special_conditions", thisStorage.getString("special_conditions"))); }
            if(thisStorage.has("type")){ props.add(new FDADeviceProperty("type", thisStorage.getString("type"), "storage_type")); }
            //Write the device storage object to the database
            String thisStorageInsertSql = fdaDevicePropertyQueryBuilder("fda_data_device_storage", fdaId, props);
            PreparedStatement storageQuery = database.prepareStatement(thisStorageInsertSql);
            database.nonQuery(storageQuery);
        }
    }

    private String fdaDevicePropertyQueryBuilder(String tableName, String fdaId, List<FDADeviceProperty> props){
        //Method to build an insert nonquery string from properties parsed from a data object
        String writeDeviceColumnsSql = "INSERT INTO " + tableName + "(fda_id";
        String writeDeviceValuesSql = ") VALUES ('" + fdaId + "'";
        for (FDADeviceProperty thisProperty : props) {
            //Loop through each property and add it to the nonquery
            writeDeviceColumnsSql += ", " + thisProperty.getColumnName();
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
