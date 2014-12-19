
/**
 * partition for NewRowColStoreEng
 *
 * @author Alan Lu
 */
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class partition{
    protected int  UNDEFINED= -1111111; //represent NULL
    protected byte [] UndefByte= new byte[1];
    protected String separator=":"; // special character to seprate key and type, use : for now
    
    //---------------- Meta-data that stores keys and object ids ------------------------//
    int numObject;
    int[] objectIds; //list of object ids stored in this partition
    String[] keyForString;
    String[] keyForLong;
    String[] keyForDouble;
    String[] keyForBool;
    
    //----------- Actual tables that store the data in this partition in memory -----------//
    long[] longValues;
    double[] doubleValues;
    byte[] booleanValues;
    long[] stringValues;    //stores location to the stringBuffer there the actual string is store
    
    //partition constructor, given a partition definition and max object size, initialize tables and meta data
    public partition(String partition, int max_size){
        //max_size is the maximum number of Objects, NOT max bytes
        String[] keyTypes = partition.split("\\s+"); //separted by any white space
        
        List<String> listStringKeys = new ArrayList<String>();
        List<String> listLongKeys = new ArrayList<String>();
        List<String> listDoubleKeys = new ArrayList<String>();
        List<String> listBoolKeys = new ArrayList<String>();
        
        for(int i = 0; i < keyTypes.length; i++){
            String[] parts = keyTypes[i].split(separator);
            if(parts[1].equals("STRING"))
                listStringKeys.add(keyTypes[i]);
            else if(parts[1].equals("LONG"))
                listLongKeys.add(keyTypes[i]);
            else if(parts[1].equals("DOUBLE"))
                listDoubleKeys.add(keyTypes[i]);
            else if(parts[1].equals("BOOL"))
                listBoolKeys.add(keyTypes[i]);
        }
        
        keyForString = listStringKeys.toArray(new String[listStringKeys.size()]);
        keyForLong = listLongKeys.toArray(new String[listLongKeys.size()]);
        keyForDouble = listDoubleKeys.toArray(new String[listDoubleKeys.size()]);
        keyForBool = listBoolKeys.toArray(new String[listBoolKeys.size()]);
        
        longValues = new long[max_size];
        doubleValues = new double[max_size];
        booleanValues = new byte[max_size];
        stringValues = new long[max_size];
        
        objectIds= new int[max_size];
        numObject = 0;
        UndefByte[0] = -1;
    }
    
    public void insertObject(int objid, Hashtable<String, Long> tempLongs, Hashtable<String, Double> tempDoubles, Hashtable<String, String> tempStrings, Hashtable<String, String> tempBool, ByteBuffer stringBuffer){
        if((keyForString == null) && (keyForLong == null) && (keyForDouble == null) && (keyForBool == null)){
            System.out.println("please contruct this partition first.");
            return;
        }
        if(numObject == objectIds.length){
            System.out.println("Full. No inserts any more!");
            return;
        }
        
        boolean emptyRow = true;
        
        for(int i = 0; i < keyForString.length; i++){
            if(tempStrings.get(keyForString[i]) != null){
                stringValues[(keyForString.length * numObject + i)] = stringBuffer.position();
                stringBuffer.putInt(tempStrings.get(keyForString[i]).length());
                stringBuffer.put(tempStrings.get(keyForString[i]).getBytes());
                emptyRow = false;
            }else{
                stringValues[(keyForString.length * numObject + i)] = UNDEFINED;
            }
        }
        
        for(int i = 0; i < keyForLong.length; i++){
            if(tempLongs.get(keyForLong[i]) != null){
                longValues[(keyForLong.length * numObject + i)] = tempLongs.get(keyForLong[i]);
                emptyRow = false;
            }else{
                longValues[(keyForLong.length * numObject + i)] = UNDEFINED;
            }
        }
        
        for(int i = 0; i < keyForDouble.length; i++){
            if(tempDoubles.get(keyForDouble[i]) != null){
                doubleValues[(keyForDouble.length * numObject + i)] = tempDoubles.get(keyForDouble[i]);
                emptyRow = false;
            }else{
                doubleValues[(keyForDouble.length * numObject + i)] = UNDEFINED;
            }
        }
        
        for(int i = 0; i < keyForBool.length; i++){
            if(tempBool.get(keyForBool[i]) != null){
                if(tempBool.get(keyForBool[i]).equals("TRUE")){
                    booleanValues[(keyForBool.length * numObject + i)] = (byte)1;
                }else if(tempBool.get(keyForBool[i]).equals("FALSE")){
                    booleanValues[(keyForBool.length * numObject + i)] = (byte)0;
                }
                emptyRow = false;
            }else{
                booleanValues[(keyForBool.length * numObject + i)] = UndefByte[0];
            }
        }
        
        if(emptyRow == false){
            objectIds[numObject] = objid;
            numObject++;
        }
        return;
    }
    
    public void getObjectFromPartition(int objid){
        for(int i = 0; i < numObject; i++){
            if(objectIds[i] > objid){
                return;
            }
            if(objectIds[i] == objid){
                //this partition contains the object, which is the i'th object in this partition
                for(int j = 0; j < keyForString.length; j++){
                    System.out.println(keyForString[j] + ":" + stringValues[i*keyForString.length + j]);
                }
                for(int j = 0; j < keyForLong.length; j++){
                    System.out.println(keyForLong[j] + ":" + longValues[i*keyForLong.length + j]);
                }
                for(int j = 0; j < keyForDouble.length; j++){
                    System.out.println(keyForDouble[j] + ":" + doubleValues[i*keyForDouble.length + j]);
                }
                for(int j = 0; j < keyForBool.length; j++){
                    System.out.println(keyForBool[j] + ":" + booleanValues[i*keyForBool.length + j]);
                }
            }
        }
        return;
    }
    
    public void selectString(byte[][] columns, ResultSet result){
        boolean[] stringAccess = new boolean[keyForString.length];
        boolean haveFields = false;
        for(int i = 0; i < keyForString.length; i++){
            stringAccess[i] = false;
            for(int j = 0; j < columns.length; j++){
                if(keyForString[i].equals( new String(columns[j]) + separator + "STRING" )){
                    stringAccess[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keyForString.length;
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            for(int j = 0; j < stringAccess.length; j++){
                if(stringAccess[j]){
                    long pos = stringValues[i*numOfFields + j];
                    String key = keyForString[j];
                    if(pos != UNDEFINED)
                        result.addString(key, oid, pos);
                }
            }
        }
        return;
    }
    
    public void selectLong(byte[][] columns, ResultSet result){
        boolean[] longAccess = new boolean[keyForLong.length];
        boolean haveFields = false;
        for(int i = 0; i < keyForLong.length; i++){
            longAccess[i] = false;
            for(int j = 0; j < columns.length; j++){
                if(keyForLong[i].equals( new String(columns[j]) + separator + "LONG" )){
                    longAccess[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keyForLong.length;
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            for(int j = 0; j < longAccess.length; j++){
                if(longAccess[j]){
                    long longnum = longValues[i*numOfFields + j];
                    String key = keyForLong[j];
                    if(longnum != UNDEFINED)
                        result.addLong(key, oid, longnum);
                }
            }
        }
        return;
    }
    
    public void selectDouble(byte[][] columns, ResultSet result){
        boolean[] doubleAccess = new boolean[keyForDouble.length];
        boolean haveFields = false;
        for(int i = 0; i < keyForDouble.length; i++){
            doubleAccess[i] = false;
            for(int j = 0; j < columns.length; j++){
                if(keyForDouble[i].equals( new String(columns[j]) + separator + "DOUBLE" )){
                    doubleAccess[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keyForDouble.length;
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            for(int j = 0; j < doubleAccess.length; j++){
                if(doubleAccess[j]){
                    double num = doubleValues[i*numOfFields + j];
                    String key = keyForDouble[j];
                    if(num != UNDEFINED)
                        result.addDouble(key, oid, num);
                }
            }
        }
        return;
    }
    
    public void selectBool(byte[][] columns, ResultSet result){
        boolean[] boolAccess = new boolean[keyForBool.length];
        boolean haveFields = false;
        for(int i = 0; i < keyForBool.length; i++){
            boolAccess[i] = false;
            for(int j = 0; j < columns.length; j++){
                if(keyForBool[i].equals( new String(columns[j]) + separator + "BOOL" )){
                    boolAccess[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keyForBool.length;
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            for(int j = 0; j < boolAccess.length; j++){
                if(boolAccess[j]){
                    byte bool = booleanValues[i*numOfFields + j];
                    String key = keyForBool[j];
                    if(bool != UndefByte[0])
                        result.addBool(key, oid, bool);
                }
            }
        }
        return;
    }
    
    public void select(byte[][] columns, ResultSet result){
        if(keyForString.length != 0)
            selectString(columns, result);
        if(keyForLong.length != 0)
            selectLong(columns, result);
        if(keyForDouble.length != 0)
            selectDouble(columns, result);
        if(keyForBool.length != 0)
            selectBool(columns, result);
        return;
    }
    
    public void selectConditionString(List<Integer> oidList, byte[][] columns, ResultSet result){
        boolean[] stringAccess = new boolean[keyForString.length];
        boolean haveFields = false;
        for(int i = 0; i < keyForString.length; i++){
            stringAccess[i] = false;
            for(int j = 0; j < columns.length; j++){
                if(keyForString[i].equals( new String(columns[j]) + separator + "STRING" )){
                    stringAccess[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keyForString.length;
        int index = 0;
        int targetId = oidList.get(index);
        
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            
            if(oid == targetId){
                for(int j = 0; j < stringAccess.length; j++){
                    if(stringAccess[j]){
                        long pos = stringValues[i*numOfFields + j];
                        String key = keyForString[j];
                        if(pos != UNDEFINED)
                            result.addString(key, oid, pos);
                        //else{
                        //    result.addString(key, oid, -1);
                        //}
                    }
                }
            }else if(oid > targetId){
                i--;
                index++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break;
            }
        }
        return;
    }
    
    public void selectConditionLong(List<Integer> oidList, byte[][] columns, ResultSet result){
        boolean[] longAccess = new boolean[keyForLong.length];
        boolean haveFields = false;
        for(int i = 0; i < keyForLong.length; i++){
            longAccess[i] = false;
            for(int j = 0; j < columns.length; j++){
                if(keyForLong[i].equals( new String(columns[j]) + separator + "LONG" )){
                    longAccess[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keyForLong.length;
        int index = 0;
        int targetId = oidList.get(index);
        
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            
            if(oid == targetId){
                for(int j = 0; j < longAccess.length; j++){
                    if(longAccess[j]){
                        long num = longValues[i*numOfFields + j];
                        String key = keyForLong[j];
                        if(num != UNDEFINED)
                            result.addLong(key, oid, num);
                    }
                }
            }else if(oid > targetId){
                i--;
                index++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break;
            }
        }
        return;
    }
    
    public void selectConditionDouble(List<Integer> oidList, byte[][] columns, ResultSet result){
        boolean[] doubleAccess = new boolean[keyForDouble.length];
        boolean haveFields = false;
        for(int i = 0; i < keyForDouble.length; i++){
            doubleAccess[i] = false;
            for(int j = 0; j < columns.length; j++){
                if(keyForDouble[i].equals( new String(columns[j]) + separator + "DOUBLE" )){
                    doubleAccess[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keyForDouble.length;
        int index = 0;
        int targetId = oidList.get(index);
        
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            
            if(oid == targetId){
                for(int j = 0; j < doubleAccess.length; j++){
                    if(doubleAccess[j]){
                        double num = doubleValues[i*numOfFields + j];
                        String key = keyForDouble[j];
                        if(num != UNDEFINED)
                            result.addDouble(key, oid, num);
                    }
                }
            }else if(oid > targetId){
                i--;
                index++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break;
            }
        }
        return;
    }
    
    public void selectConditionBool(List<Integer> oidList, byte[][] columns, ResultSet result){
        boolean[] boolAccess = new boolean[keyForBool.length];
        boolean haveFields = false;
        for(int i = 0; i < keyForBool.length; i++){
            boolAccess[i] = false;
            for(int j = 0; j < columns.length; j++){
                if(keyForBool[i].equals( new String(columns[j]) + separator + "BOOL" )){
                    boolAccess[i] = true;
                    haveFields = true;
                    break;
                }
            }
        }
        if(haveFields == false) return;
        
        int numOfFields = keyForBool.length;
        int index = 0;
        int targetId = oidList.get(index);
        
        for(int i = 0; i < numObject; i++){
            int oid=objectIds[i];
            
            if(oid == targetId){
                for(int j = 0; j < boolAccess.length; j++){
                    if(boolAccess[j]){
                        byte bool = booleanValues[i*numOfFields + j];
                        String key = keyForBool[j];
                        if(bool != UndefByte[0])
                            result.addBool(key, oid, bool);
                    }
                }
            }else if(oid > targetId){
                i--;
                index++;
                if(index < oidList.size())
                    targetId = oidList.get(index);
                else
                    break;
            }
        }
        return;
    }
    
    public void selectCondition(List<Integer> oidList, byte[][] columns, ResultSet result){
        if(keyForString.length != 0)
            selectConditionString(oidList, columns, result);
        if(keyForLong.length != 0)
            selectConditionLong(oidList, columns, result);
        if(keyForDouble.length != 0)
            selectConditionDouble(oidList, columns, result);
        if(keyForBool.length != 0)
            selectConditionBool(oidList, columns, result);
        
        return;
    }
    
    //this is the partition with where field
    public List<Integer> selectRangeWherePar(byte[][] selectCols, byte[] whereCol, long value1, long value2, ResultSet result){
        List<Integer> oidList = new ArrayList<Integer>();
        
        //assuming the where col is in type LONG
        boolean[] access = new boolean[keyForLong.length];
        int whereIndex = -1;
        
        for(int i = 0; i < keyForLong.length; i++){
            if(keyForLong[i].equals( new String(whereCol) + separator + "LONG" )) whereIndex = i;
            
            access[i] = false;
            for(int j = 0; j < selectCols.length; j++){
                if(keyForLong[i].equals( new String(selectCols[j]) + separator + "LONG" )){
                    access[i] = true;
                    break;
                }
            }
        }
        
        int numOfFields = keyForLong.length;
        for(int i = 0; i < numObject; i++){
            //first check if this object meet the where condition
            long value = longValues[i*numOfFields + whereIndex];
            if( (value >= value1) && (value <= value2)){
                //this object meets condition
                int oid=objectIds[i];
                oidList.add(oid);
                
                //select fields in the select clause
                for(int j = 0; j < access.length; j++){
                    if(access[j]){
                        String key = keyForLong[j];
                        if(j == whereIndex){
                            result.addLong(key, oid, value);
                            continue;
                        }
                        long longnum = longValues[i*numOfFields + j];
                        if(longnum != UNDEFINED)
                            result.addLong(key, oid, longnum);
                    }
                }
            }
        }
        
        if(oidList.size() > 0){
            //if this partition also has other data types, also select those based on oidList
            if(keyForString.length != 0)
                selectConditionString(oidList, selectCols, result);
            if(keyForDouble.length != 0)
                selectConditionDouble(oidList, selectCols, result);
            if(keyForBool.length != 0)
                selectConditionBool(oidList, selectCols, result);
        }
        return oidList;
    }
}
