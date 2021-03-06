
/**
 * Save JSON file into memory based column store
 * Every JSON field is saved into a column, represented by a buffer
 *
 * Assumption: we don't have synchronization so far
 *             so no need to protect about buffer array acess
 *
 * @author Jin Chen, Alan Lu
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
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.*;

import java.util.concurrent.atomic.AtomicInteger;

public class NewColStoreEngParallel extends StoreEngine  {

    /* This class is different from "Column" class in that it store all data types in one array of the type long.
    *  String: store position (long) to the stringbuffer into the main array table
    *  Long: store as long
    *  Boolean: Store as long - True as 1 and false as 0
    *  Double: same as the SCUBA paper
    */
    public class ColumnV2{
        AtomicInteger numObject;
        int[] objectIds;
        String key;
        String type;
        
        int lockPolicy = 0;
		Lock fineLock;
		ReadWriteLock fineReadWriteLock;

        //----------- Actual tables that store the data in this partition in memory -----------//
        long[] data;

        public ColumnV2(String columnKey, int max_size, int lock_policy){

            numObject = new AtomicInteger();

            //max_size is the maximum number of Objects, NOT max bytes

            //assuming columnKey is in the format of "KEY:TYPE"
            String[] parts = columnKey.split(separator);

            if(parts[1].equals("STRING")){
                type = "STRING";
            } else if(parts[1].equals("LONG")){
                type = "LONG";
            } else if(parts[1].equals("DOUBLE")){
                type = "DOUBLE";
            } else if(parts[1].equals("BOOL")){
                type = "BOOL";
            }

            data = new long[max_size];
            key = new String(columnKey);
            objectIds= new int[max_size];
            
			lockPolicy = lock_policy;
            if (lockPolicy == 20)
    			fineLock = new ReentrantLock(); 
			else if (lockPolicy == 21)
				fineReadWriteLock = new ReentrantReadWriteLock();
        }

        public void saveStrValue(int objid, long pos){
        
			lockMe('w');
			//if (enableColLock)
        	//	fineLock.lock();

            int numObject = this.numObject.getAndIncrement();
            data[numObject] = pos;
            objectIds[numObject] = objid;
			//if (objid >= 200000)
			//{
			//	System.out.println(key + ": added " + objid);
			//	try {
			//		Thread.sleep(10);
			//	}
			//	catch (Exception e) {
			//	}
			//}
				
         	//if (enableColLock)
        	//	fineLock.unlock();        
			unlockMe('w');
			
            return;
        }

        public void saveLongValue(int objid, long longnum){
        
			lockMe('w');
         	//if (enableColLock)
        	//	fineLock.lock();
        		       
            int numObject = this.numObject.getAndIncrement();
            data[numObject] = longnum;
            objectIds[numObject] = objid;
            
            //if (enableColLock)
        	//	fineLock.unlock();   
            unlockMe('w');
			
            return;
        }

        public void saveDoubleValue(int objid, double num){
        
			lockMe('w');
          	//if (enableColLock)
        	//	fineLock.lock();       
        
            int numObject = this.numObject.getAndIncrement();
            data[numObject] = (long) num * 100000;
            objectIds[numObject] = objid;
            
          	//if (enableColLock)
        	//	fineLock.unlock();              
            unlockMe('w');
			
            return;
        }

        public void saveBoolValue(int objid, byte bool){
        
			lockMe('w');
          	//if (enableColLock)
        	//	fineLock.lock();         
        
            int numObject = this.numObject.getAndIncrement();
            if(bool == (byte)1){
                data[numObject] = (long) 1;
            } else if(bool == (byte)0){
                data[numObject] = (long) 0;
            }
            objectIds[numObject] = objid;
            
           	//if (enableColLock)
        	//	fineLock.unlock();            
            unlockMe('w');
			
            return;
        }

        public void selectString(ResultSetV2 result){
		
			lockMe('r');
		
            for(int i = 0; i < numObject.get(); i++){
                int oid=objectIds[i];
                long pos = data[i];
                result.addLong(key, oid, pos);
            }
			
			unlockMe('r');
			
            return;
        }

        public void selectLong(ResultSetV2 result){
		
			lockMe('r');
		
            for(int i = 0; i < numObject.get(); i++){
                int oid=objectIds[i];
                long longnum = data[i];
                result.addLong(key, oid, longnum);
            }
            
			unlockMe('r');
			
			return;
        }

        public void selectDouble(ResultSetV2 result){
		
			lockMe('r');
		
            for(int i = 0; i < numObject.get(); i++){
                int oid=objectIds[i];
                long num = data[i];
                result.addLong(key, oid, num);
            }
            
			unlockMe('r');
			
			return;
        }

        public void selectBool(ResultSetV2 result){
		
			lockMe('r');
		
            for(int i = 0; i < numObject.get(); i++){
                int oid=objectIds[i];
                long boolVal = data[i];
                result.addLong(key, oid, boolVal);
            }
            
			unlockMe('r');
			
			return;
        }

        //this is the column with where field, assume this is only called if this column is long
        public List<Integer> selectRangeWhereCol(byte[][] selectCols, long value1, long value2, ResultSetV2 result){
		
			lockMe('r');
		
            List<Integer> oidList = new ArrayList<Integer>();

            boolean alsoSelect = false;
            //This is the WHERE col, but also check if we should also select this col
            for(int i = 0; i < selectCols.length; i++){
                if(key.equals( new String(selectCols[i]) + separator + "LONG" )){
                    alsoSelect = true;
                    break;
                }
            }

            for(int i = 0; i < numObject.get(); i++){
                long value = data[i];
                if( (value >= value1) && (value <= value2)){
                    //this object meets condition
                    int oid = objectIds[i];
                    oidList.add(oid);

                    if(alsoSelect){
                        result.addLong(key, oid, value);
                    }
                }
            }
			
			unlockMe('r');
			
            return oidList;
        }

        //given a list of oid, select all objects with oids in the list
        public void selectCondition(List<Integer> oidList, ResultSetV2 result){
		
			lockMe('r');
		
            int index = 0;
            int targetId = oidList.get(index);

            for(int i = 0; i < numObject.get(); i++){
                int oid = objectIds[i];

                if(oid == targetId){
                    long longnum = data[i];
                    result.addLong(key,oid,longnum);
                }else if(oid > targetId){
                    i--;
                    index++;
                    if(index < oidList.size())
                        targetId = oidList.get(index);
                    else
                        break;
                }
            }
			
			unlockMe('r');
			
            return;
        }

		public void lockMe(char opType)
		{
			if (lockPolicy == 20)
			{
				fineLock.lock();
			}
			else if (lockPolicy == 21)
			{
				if (opType == 'r')
					fineReadWriteLock.readLock().lock();
				else if (opType == 'w')
					fineReadWriteLock.writeLock().lock();
			}
		
			return;
		}
		
		public void unlockMe(char opType)
		{
			if (lockPolicy == 20)
			{
				fineLock.unlock();
			}
			else if (lockPolicy == 21)
			{
				if (opType == 'r')
					fineReadWriteLock.readLock().unlock();
				else if (opType == 'w')
					fineReadWriteLock.writeLock().unlock();
			}		
		
			return;
		}	
		
    } // end of the ColumnV2 Class


    private ByteBuffer stringBuffer;
    //ResultSetV2 result;
    ConcurrentHashMap<Long, ResultSetV2> resultSets;
    //Map<Long, ResultBriefStats> briefStats;
    int numThreads;
    HashMap<String, ColumnV2> cols;

    //Note: This used to be 100*1000*1000. Changed to 2 million to make it easier to test.
    private static final int RESULT_SET_SIZE = 2*1000*1000;

    public NewColStoreEngParallel(int memory_size_in_bytes, int numThreads, String lockMethod)
    {
        //call parent constructor
        super(memory_size_in_bytes);
        /* create colum Buffer hash table */
        //colBufs = new Hashtable<String, ByteBuffer>();

        cols = new HashMap<String, ColumnV2>();
        stringBuffer = ByteBuffer.allocateDirect(max_buf_size*100);

        stringBuffer.position(0);
        this.numThreads = numThreads;
        this.resultSets = new ConcurrentHashMap<Long, ResultSetV2>();
        
        // select lock policy
        if (lockMethod.equalsIgnoreCase("CoarseLock")) // coarse lock at store level
        	lockPolicy = 10;
		else if (lockMethod.equalsIgnoreCase("FineLock"))	// fine lock at column level
			lockPolicy = 11;
		else if (lockMethod.equalsIgnoreCase("CoarseReadWriteLock"))	// fine lock at column level
			lockPolicy = 20;
		else if (lockMethod.equalsIgnoreCase("FineReadWriteLock"))	// fine lock at column level
			lockPolicy = 21;
			
		
		if (lockPolicy == 10)
			coarseLock = new ReentrantLock(); 
		else if (lockPolicy == 11)
			coarseReadWriteLock = new ReentrantReadWriteLock();
        		
    }

    /**
     * Navigate the json object and parse it and save it into storage
     */
    public void insertObject(int objid, JsonValue tree, String key){
		//System.out.println(objid);
		//if (enableCoarseGrainedLock)
		//{
		//	
		//	coarseLock.lock();	
		//	//System.out.println("Lock - " + objid);
		//	insertObjectLockProtected(objid, tree, key);
		//	//System.out.println("Unlock - " + objid);
		//	coarseLock.unlock();
		//}
		//else
		//{
		//	insertObjectLockProtected(objid, tree, key);
		//}
		
		lockMe('w');
		insertObjectLockProtected(objid, tree, key);
		unlockMe('w');
		
    }

    public void insertObjectLockProtected(int objid, JsonValue tree, String key){

        switch(tree.getValueType()){
            case OBJECT:
                JsonObject object = (JsonObject) tree;
                for(String name: object.keySet()){
                    if(key!=null)
                        insertObjectLockProtected(objid,object.get(name),key+"."+name);
                    else
                        insertObjectLockProtected(objid,object.get(name),name);
                }
                break;
            case ARRAY:
                JsonArray array = (JsonArray) tree;
                int index =0;
                for (JsonValue val : array){
                    insertObjectLockProtected(objid,val,key+"["+index+"]");
                    index += 1;
                }
                break;
            case STRING:
                JsonString st = (JsonString) tree;
                saveStrValue(objid,key,st.getString());
                break;
            case NUMBER:
                JsonNumber num = (JsonNumber) tree;
                if(num.isIntegral()){
                    saveLongValue(objid,key,num.longValue());
                }else{
                    saveDoubleValue(objid,key,num.doubleValue());
                }
                break;
            case TRUE:
            case FALSE:
                saveBoolValue(objid,key,tree.getValueType().toString());
                break;
            case NULL:
                // we didn't save null value
                break;
        }
    }

    protected int saveStrValue(int objid, String key, String value)
    {
        //later -- use dictionary to compress
        // save to str
        String bufkey = key+separator+"STRING";

        if(cols.get(bufkey) == null){
            int size = max_buf_size/8;
            //Temporary - for running sparse data on nobench_data.json
            if(key.contains("sparse_")){
                size = max_buf_size/8/100;
            }
            cols.put(bufkey, new ColumnV2(bufkey, size, lockPolicy));
        }

        long position = stringBuffer.position();
        cols.get(bufkey).saveStrValue(objid, position);
        stringBuffer.putInt(value.length());
        stringBuffer.put(value.getBytes());

        return 1;
    }

    protected int saveLongValue(int objid, String key, long num)
    {
        String bufkey = key+separator+"LONG";

        if(cols.get(bufkey) == null){
            int size = max_buf_size/8;
            //Temporary - for running sparse data on nobench_data.json
            if(key.contains("sparse_")){
                size = max_buf_size/8/100;
            }
            cols.put(bufkey, new ColumnV2(bufkey, size, lockPolicy));
        }

        cols.get(bufkey).saveLongValue(objid, num);

        return 1;
    }

    protected int saveDoubleValue(int objid, String key, double num)
    {
        String bufkey = key+separator+"DOUBLE";

        if(cols.get(bufkey) == null){
            int size = max_buf_size/8;
            //Temporary - for running sparse data on nobench_data.json
            if(key.contains("sparse_")){
                size = max_buf_size/8/100;
            }
            cols.put(bufkey, new ColumnV2(bufkey, size, lockPolicy));
        }

        cols.get(bufkey).saveDoubleValue(objid, num);

        return 1;
    }

    protected int saveBoolValue(int objid, String key, String value)
    {
        String bufkey = key+separator+"BOOL";

        if(cols.get(bufkey) == null){
            int size = max_buf_size/8;
            //Temporary - for running sparse data on nobench_data.json
            if(key.contains("sparse_")){
                size = max_buf_size/8/100;
            }
            cols.put(bufkey, new ColumnV2(bufkey, size, lockPolicy));
        }

        if(value.equals("TRUE")==true){
            cols.get(bufkey).saveBoolValue(objid, (byte)1);
        }else if(value.equals("FALSE")==true){
            cols.get(bufkey).saveBoolValue(objid, (byte)0);
        }else{
            System.out.println("Error: unknow value "+value);
        }

        return 1;
    }


    /** TODO: finish this
     */

    public int getObject(int targetId){
        return 0;
    }

    public void select(byte[][] columns){
		lockMe('r');
	

        long tId = Thread.currentThread().getId();
        ResultSetV2 result = resultSets.get(tId);
        result.clearResultSet();

        for(int i = 0; i< columns.length; i++){
            //ByteBuffer selectBuf;

            // since we don't know type, we try STRING ,LONG,DOUBLE,BOOL -- could be dyn type
            String selectKey = new String(columns[i]) + separator + "STRING";
            if(cols.get(selectKey) != null)
                cols.get(selectKey).selectString(result);

            selectKey = new String(columns[i]) + separator + "LONG";
            if(cols.get(selectKey) != null)
                cols.get(selectKey).selectLong(result);

            selectKey = new String(columns[i]) + separator + "DOUBLE";
            if(cols.get(selectKey) != null)
                cols.get(selectKey).selectDouble(result);

            selectKey = new String(columns[i]) + separator + "BOOL";
            if(cols.get(selectKey) != null)
                cols.get(selectKey).selectBool(result);

        } //end for

        //copy enough info for brief stats
//        long queryId = Long.parseLong(Thread.currentThread().getName());
//        ResultBriefStats toAdd = new ResultBriefStats(result.index, result.oids);
//        briefStats.put(queryId, toAdd);

		unlockMe('r');

        return;
    }

    /* select x,y,z,... where a between value1 and value2
     * range query, single column, long  type -- need to extend its type to include double
     */
    public void selectRange(byte[][] selectCols, byte[] whereCol, long value1, long value2)
    {
		lockMe('r');
	
        long tId = Thread.currentThread().getId();
        //System.out.println("thread id in selectRange: " + tId);
        ResultSetV2 result = resultSets.get(tId);
        result.clearResultSet();
//        if( resultSets.containsKey(tId) ){
//            result = resultSets.get(tId);
//            result.clearResultSet();
//        } else {
//            result = addNewResultSet(tId);
////            synchronized(resultSets){
////                result = new ResultSetV2(RESULT_SET_SIZE);//resultSets.get(tId);
////                resultSets.put(tId, result);
////                System.out.println("inserting resultset for tId: " + tId);
////            }
//        }


        String where = (new String(whereCol)) + separator + "LONG";
        List<Integer> oidList = cols.get( where ).selectRangeWhereCol(selectCols, value1, value2, result);

        if(oidList.size() > 0){
            Hashtable<String, String> colsToSelect = new Hashtable<String, String>();

            if(selectCols[0][0]==(byte) '*' ){//select all fields
                for(String col: cols.keySet()){
                    colsToSelect.put(col, "");
                }
            }else{
                for(byte[] select: selectCols){
                    String selectField = new String(select);
                    if(cols.get(selectField + separator + "LONG") != null)
                        colsToSelect.put(selectField + separator + "LONG", "");

                    if(cols.get(selectField + separator + "DOUBLE") != null)
                        colsToSelect.put(selectField + separator + "DOUBLE", "");

                    if(cols.get(selectField + separator + "STRING") != null)
                        colsToSelect.put(selectField + separator + "STRING", "");

                    if(cols.get(selectField + separator + "BOOL") != null)
                        colsToSelect.put(selectField + separator + "BOOL", "");
                }
            }

            //the column that contains the WHERE has been already selected as we checked condition
            colsToSelect.remove( where );

            for(String col: colsToSelect.keySet()){
                cols.get(col).selectCondition(oidList, result);
            }
        }

		unlockMe('r');

        return;
    }


    /* TODO: Finish this
     select x,y,z where a = ANY xx
     * xx is a set, single column and single relation parsing
     * Method:  there are multiple columns for this set xx,
     *         scan all of where columns and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     *        assume string type for now
     */
    public HashMap<Integer, HashMap<String, String>>  selectWhereAny(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
		lockMe('r');
        HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
		unlockMe('r');
		
        return resultSet;
    }


    /* TODO: Finish this
     select x,y,z where a = xx or a < xx or a > xx
     * single column and single relation parsing
     * Method: scan the where column and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     */
    public HashMap<Integer, HashMap<String, String>> selectWhereSingle(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
	
		lockMe('r');
        HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
		unlockMe('r');
		
        return resultSet;
    }

    public void printBriefStats(){
        //result.printBriefStats();
        return;
    }

    //function is unused
    public void printBriefStatsByThread() {
        long queryId = Long.parseLong(Thread.currentThread().getName());
    //    resultSets.get(queryId).printBriefStats();
    }

    //function is unused
    public void setNumberOfThreads(int numThreads) {
        for( long i = 0; i < numThreads; i++ ){
    //        resultSets.put(i, new ResultSetV2(RESULT_SET_SIZE) );
        }
    }


    public String getBriefStatsString() {
        long queryId = Long.parseLong(Thread.currentThread().getName());
        long tId = Thread.currentThread().getId();
        String toReturn = resultSets.get(tId).makeBriefStatsString();
        return toReturn;
    }


    public ResultSetV2 addNewResultSet(long tId ){
        System.out.println("== Creating resultsetv2");
        ResultSetV2 rs = new ResultSetV2(RESULT_SET_SIZE);
        System.out.println("== Done creating resultsetv2");
        resultSets.put(tId, rs);
        System.out.println("done inserting resultset for tId: " + tId);
        return rs;
    }
	
	public void lockMe(char opType)
	{
		if (lockPolicy == 10)
		{
			coarseLock.lock();
		}
		else if (lockPolicy == 11)
		{
			if (opType == 'r')
				coarseReadWriteLock.readLock().lock();
			else if (opType == 'w')
				coarseReadWriteLock.writeLock().lock();
		}
	
		return;
	}
	
	public void unlockMe(char opType)
	{
		if (lockPolicy == 10)
		{
			coarseLock.unlock();
		}
		else if (lockPolicy == 11)
		{
			if (opType == 'r')
				coarseReadWriteLock.readLock().unlock();
			else if (opType == 'w')
				coarseReadWriteLock.writeLock().unlock();
		}		
	
		return;
	}	

}
