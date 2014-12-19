
/**
 *
 * Store engine for saving JSON data 
 * This is a parent class for colstore, rowstore, rowcolstore,etc.
 *
 * Assumption: we don't have synchronization so far   
 *             so no need to protect about buffer array acess 
 *
 * @author Jin Chen, Alan Lu
 *
 */

import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicInteger;


public class StoreEngine {

	// atomic integer to track number of objects in store
	public AtomicInteger objCounter = new AtomicInteger(0);

    protected int max_buf_size;

    protected int  UNDEFINED= -1111111; //represent NULL  
    protected byte [] UndefByte= new byte[1];

    /* special character to seprate key and type, use : for now*/ 
    protected String separator=":";


    /**
     * Creates a ring buffer, and using Column format to store the data
     * initialize the buffer size 
     */
    public StoreEngine(int memory_size_in_bytes)
    {
        /* later -- pass it from paramters */
        max_buf_size = memory_size_in_bytes; /* allocated first */
        UndefByte[0] = -1;

    }

    /**
     * Fixed row format [objid,keystr,valstr,valnum,valbool] 
     */

    public int getObject(int targetId)
    {
        return 0;
    }

    protected int saveStrValue(int objid, String key, String value)
    {   
        return 0;
    }
    protected int saveLongValue(int objid, String key, long num)
    {
        return 0;
    }
    
    protected int saveDoubleValue(int objid, String key, double num)
    {
        return 0;
    }

    protected int saveBoolValue(int objid, String key, String value)
    {
        return 0;
    }

    public double getRowIdRatio(){
        return 0.0;
    }
    
    public double getNullRatio(){
        return 0.0;
    }
    
    public void printBriefStats(){
        return;
    }
    
    public void printBriefStatsByThread(){
        return;
    }
    
    public void printLayoutInfo(){
        return;
    }

    public void insertObject(int objid, JsonValue tree, String key){

        switch(tree.getValueType()){
            case OBJECT:
                //System.out.println("  OBJECT");
                JsonObject object = (JsonObject) tree;
                for(String name: object.keySet()){
                    if(key!=null)
                        insertObject(objid,object.get(name),key+"."+name);
                    else
                        insertObject(objid,object.get(name),name);
                }
                //if((objid % 10000) == 1) 
                //   System.out.println("Row id " + objid+ "buffer offset "+buffer.position());
                break;
            case ARRAY:
                //System.out.println("  ARRAY");
                JsonArray array = (JsonArray) tree;
                int index =0; 
                for (JsonValue val : array){
                    insertObject(objid,val,key+"["+index+"]");
                    index += 1;
                }
                break;
            case STRING:
                JsonString st = (JsonString) tree;
                saveStrValue(objid,key,st.getString());
                //System.out.println(objid+" "+key+" "+st.getString());
                break;
            case NUMBER:
                JsonNumber num = (JsonNumber) tree;
                if(num.isIntegral()){
                    saveLongValue(objid,key,num.longValue());
                }else{
                    saveDoubleValue(objid,key,num.doubleValue());
                }
                //System.out.println(objid+" "+key+" "+num.toString());
                break;
            case TRUE:
            case FALSE:
                saveBoolValue(objid,key,tree.getValueType().toString());
                //System.out.println(objid+" "+key+" "+tree.getValueType().toString());
                break;
            case NULL:
                // we didn't save null value
                //System.out.println("null\n:");
                //System.out.println(objid+"key "+key+tree.getValueType().toString());
                break;
        }


    }


	public void select(byte[][] columns){
        return;
	}
    
    /*
     * select where value = ANY xxx
     *  xxx is a set / JASON array, need to compare each member of this set
     */ 
	public HashMap<Integer, HashMap<String, String>> selectWhereAny(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        return new HashMap<Integer, HashMap<String, String>>();
	}
    /* select x,y,z,... where a between value1 and value2
     * range query, single column, long  type 
     */
    public void selectRange(byte[][] selectCols, byte[] whereCol, long value1, long value2)
    {
        return;
    }
     
    /* select x,y,z where a = xx or a < xx or a > xx
     * single column and single relation parsing 
     * Method: scan the where column and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     */
    public HashMap<Integer, HashMap<String, String>> selectWhereSingle(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){    
        return new HashMap<Integer, HashMap<String, String>>();
    }

   
    /* obsolete functions */
	public long selectWhere(byte[][] selectCols, byte[] whereCol, int threshold){
        return 0;
	}

    public Hashtable<String, Integer> aggregateRangeGroupBy(byte[][] selectCols, byte[] whereCol, long value1, long value2, byte[] gColumn)
    {
        return new Hashtable<String,Integer>();
    }
    public long aggregateColumn(byte[] selectCol, byte[] whereCol, int threshold){
        return 0;
    }
    public long aggregate(byte[] colName, int threshold){
        return 0;
    }
    public void printTable(String outputfile){
        return;
    }

    public void setNumberOfThreads(int numThreads) {
        return;
    }
    
    public void printBriefStatsForQuery( long queryId ) {
        return;
    }
    
    public ResultBriefStats getBriefStatsForQuery( long queryId ) {
        return null;
    }
    
    public String getBriefStatsString() {
        return null;
    }
    
    public synchronized ResultSetV2 addNewResultSet(long tId ){
        return null;
    }
    
//    public ResultSetV2 getResultSetForQueryId( long queryId ){
//        return null;
//    }
}
