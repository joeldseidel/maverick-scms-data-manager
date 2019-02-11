public class FDADeviceProperty
{
    String keyName;
    Object value;
    String colName;

    //Constructor for when the property and the destination column name are not the same
    FDADeviceProperty(String keyName, Object value, String colName){
        this.keyName = keyName;
        this.value = value;
        this.colName = colName;
    }

    //Constructor for when the property and the destination column name are the same
    FDADeviceProperty(String keyName, Object value){
        this.keyName = keyName;
        this.value = value;
        this.colName = keyName;
    }

    String getColumnName(){
        return this.colName;
    }

    Object getValue(){
        return this.value;
    }
}
