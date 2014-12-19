
/**
 * Save JSON file into memory based column store 
 * Every JSON field is saved into a column, represented by a buffer    
 * 
 * Assumption: we don't have synchronization so far   
 *             so no need to protect about buffer array acess 
 *
 * @author Jin Chen
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



public class ColStore {

    private int max_buf_size;

    private int  UNDEFINED= -1111111; //represent NULL  
    private byte [] UndefByte= new byte[1];

    /* key+type as the key for the hash table */
    Hashtable<String, ByteBuffer> colBufs; 

    /* special character to seprate key and type, use : for now*/ 
    String separator=":";


    /**
     * Creates a ring buffer, and using Column format to store the data
     * initialize the buffer size 
     */
    public ColStore (int memory_size_in_bytes)
    {
        /* later -- pass it from paramters */
        max_buf_size = memory_size_in_bytes; /* allocated first */

        /* create colum Buffer hash table */ 
        colBufs = new Hashtable<String, ByteBuffer>();  
        //valStrBuf = ByteBuffer.allocateDirect(max_buf_size);
        //
        UndefByte[0] = -1;

    }

    /**
     * not Fixed row format [objid,keystr,valstr,valnum,valbool] 
     * objid - int, 4Bytes
     * keystr = int + chars, 4Bytes + length
     * valstr = int + chars, 4 Bytes + length
     * valnum -- using long or double (64bits, 8 bytes) - later distinguish them 
     *           8 Bytes  
     * valbool = 1 byte 
     */

    private int saveStrValue(int objid, String key, String value)
    {
        /* check whether we have buffer or not */

        //later -- use dictionary to compress
        // save to str
        String bufkey = key+separator+"STRING";
        //System.out.println(bufkey);
        ByteBuffer buf=colBufs.get(bufkey);
        if(buf == null){
            // allocate the byte buffer 
            System.out.println("allocate buf "+max_buf_size);
            buf = ByteBuffer.allocateDirect(max_buf_size);
            colBufs.put(bufkey,buf);
        }
        // insert content to buf  - no need to save key since it is in hash table
        
        if(buf != null){
            // insert  
            buf.putInt(objid);
            buf.putInt(value.length());
            buf.put(value.getBytes());
        }
        
        return 1;
        
    }

    private int saveLongValue(int objid, String key, long num)
    {
        String bufkey = key+separator+"LONG";
        ByteBuffer buf=colBufs.get(bufkey);
        if(buf == null){
            //allocate byte buffer
            //System.out.println("allocate buf "+max_buf_size);
            buf = ByteBuffer.allocateDirect(max_buf_size);
            colBufs.put(bufkey,buf);
        }
        if(buf != null){
            buf.putInt(objid);
            buf.putLong(num);
        }
        return 1;
    } 

    private int saveDoubleValue(int objid, String key, double num)
    {
        String bufkey = key+separator+"DOUBLE";
        ByteBuffer buf=colBufs.get(bufkey);
        if(buf == null){
            //allocate byte buffer
            System.out.println("allocate buf "+max_buf_size);
            buf = ByteBuffer.allocateDirect(max_buf_size);
            colBufs.put(bufkey,buf);
        }
        if(buf !=null){
            buf.putInt(objid);
            buf.putDouble(num);
        }
        return 1;
    } 

    private int saveBoolValue(int objid, String key, String value)
    {
        String bufkey = key+separator+"BOOL";
        ByteBuffer buf = colBufs.get(bufkey);
        if(buf == null){
            System.out.println("allocate buf "+max_buf_size);
            buf = ByteBuffer.allocateDirect(max_buf_size);
            colBufs.put(bufkey,buf);
        }
        if(buf !=null ){
            buf.putInt(objid);
            if(value.equals("TRUE")==true){
                buf.put((byte)1);
            }else if(value.equals("FALSE")==true){
                buf.put((byte)0);
            }else{
                System.out.println("Error: unknow value "+value);
            }
        }
        return 1;
    }

    /**
     * Fixed row format [objid,keystr,valstr,valnum,valbool] 
     */

    public int getObject(int targetId)
    {
        /* tranverse each buffer to read it */
        //get the buffer
        System.out.println("Iterating on the buffer hashtable"+colBufs.keySet());
        for(String key: colBufs.keySet()){
            //System.out.println("key:"+key);
            // check the key and assign different method
            ByteBuffer buf = colBufs.get(key);
            String [] parts = key.split(separator); 
            String columnKey = parts[0];
            String type = parts[1];
            if(type.equals("STRING")){
                System.out.println("STRING "+columnKey);
                getObjectFromStrBuf(buf,targetId);
            }else if(type.equals("LONG")){
                System.out.println("LONG "+columnKey);
                getObjectFromLongBuf(buf,targetId);
            }else if(type.equals("DOUBLE")){
                System.out.println("DOUBLE "+columnKey);
                getObjectFromDoubleBuf(buf,targetId);
            }else if(type.equals("BOOL")){
                System.out.println("BOOL "+columnKey);
                getObjectFromBoolBuf(buf,targetId);
            }else{
                System.out.println("Error: no such type in buf "+type);
                return 1;
            }
        }

        return 0;
    }

    public void getObjectFromBoolBuf(ByteBuffer buf, int targetId)
    {
        int bound = buf.position();
        ByteBuffer readBuf = buf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read bool   
            byte [] valbool = new byte[1];
            readBuf.get(valbool);
            if(oid == targetId)
                System.out.println(" " + valbool[0]);
        }
    }

    public void getObjectFromDoubleBuf(ByteBuffer buf, int targetId)
    {
        int bound = buf.position();
        ByteBuffer readBuf = buf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read double  
            double value = readBuf.getDouble();
            if(oid == targetId)
                System.out.println(" " + value);
        }
    }

    public long getObjectFromLongBuf(ByteBuffer buf, int targetId)
    {
        int bound = buf.position();
        ByteBuffer readBuf = buf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read Long  
            long value = readBuf.getLong();
            if(oid == targetId)
                return value;
                //System.out.println(" " + value);
        }
        return UNDEFINED;

    }


    public void getObjectFromStrBuf(ByteBuffer buf, int targetId)
    {
        int bound = buf.position();
        ByteBuffer readBuf = buf.asReadOnlyBuffer();
        readBuf.position(0);
        int oid = 0;
        byte [] key,valstr; 
        int len;

        while(readBuf.position()<bound){
            // read object id
            oid = readBuf.getInt();
            if(oid > targetId)
               break; // we assume the target id increase monotonously
            if(oid == targetId){
                System.out.print("Row "+oid);
            }
            // read val string 
            len = readBuf.getInt();
            valstr = new byte[len];
            readBuf.get(valstr);
            if(oid == targetId)
                System.out.println(" " + new String(valstr));
        }

    }

    /**
    *
    Table 1: objid + keystr (4B + 4B + length)
    Table 2: objid + valstr (4B + 4B + length)
    Table 3: objid + INT   (4B+4B - int array)
    Talbe 4: objid + LONG (4B + 8B
    Table 5: objid + DOUBLE
    Table 6: objid + BOOL ( 4B + 1B )  -- maybe compressed with objid together
    *
    */

    /**
    * Navigate the json object and parse it and save it into storage 
    *
    */

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

    /**
    * execute a simple aggeration query with one condition check
    * first, just do scan, then do sum up
    * get the buffer for where column, and if selected, and get data from other buffers  
    * for now, only aggregate LONG type
    */
    public long aggregate(byte[] colName, int threshold){
        long sum = 0;

        // for now, the where filed is LONG 
        String whereKey = new String(colName) + separator + "LONG";
        ByteBuffer whereBuf = colBufs.get(whereKey);
        //System.out.println("search column key"+whereKey); 

        ByteBuffer [] selectBufs = new ByteBuffer [colBufs.size()-1];
        int [] bounds = new int [colBufs.size() - 1];

        if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey); 
            return -1;
        }
        int index = 0; 
        for(String key: colBufs.keySet()){
            if( key.equals(whereKey)!=true ) {
                //System.out.println("select buffer"+key);
                ByteBuffer sbuf = colBufs.get(key);
                //record current write position 
                bounds[index]=sbuf.position();
                //create a read buffer for this column 
                ByteBuffer rBuf = sbuf.asReadOnlyBuffer(); 
                rBuf.position(0);
                selectBufs[index] = rBuf; 
                index += 1;
            }else{
                //System.out.println("where buffer"+key);
            }
        }
        
        ByteBuffer readBuf = whereBuf.asReadOnlyBuffer();
        int bound = whereBuf.position();
        readBuf.position(0); 
        //have a loop to scan each record in readBuf field
        // check selectivity first
        int oid = 0;
        while(readBuf.position()<bound){
            oid = readBuf.getInt();
            long value = readBuf.getLong();
            if(value <= threshold){
                //get other columns and sum up
                for(int i = 0; i< index ; i++){
                    //similar as getObjectFromLongBuf
                    ByteBuffer rBuf = selectBufs[i];
                    int wBound = bounds[i];
                    int cOid = 0;
                    while(rBuf.position() < bound){
                        //read current object id
                        cOid = rBuf.getInt();
                        if(cOid > oid){
                            //position back 
                            int newPosition = rBuf.position()-Integer.SIZE/Byte.SIZE; 
                            rBuf.position(newPosition);
                            break;  
                        }
                        //if(cOid == oid){
                            //System.out.print("Row "+cOid);
                        //}
                        //read Long
                        if(cOid == oid){
                            long sValue = rBuf.getLong();
                            sum = sum + sValue;
                        }else{ 
                            //skip long
                            int newPosition = rBuf.position()+Long.SIZE/Byte.SIZE; 
                            rBuf.position(newPosition);
                        }
                    } // while rBuf
                } //for index
            }//threshold
        }
        
        return sum;
        
    }
	public long fastSelect(byte[][] columns){
		long sum = 0;
		long longnum = 0;
		for(int i = 0; i < columns.length; i++){
			String selectKey = new String(columns[i]) + separator + "LONG";
			ByteBuffer selectBuf = colBufs.get(selectKey);
		
			if(selectBuf == null){
	            System.out.println("Didn't find buffer key"+selectKey); 
    	        return -1;
        	}

			int bound = selectBuf.position();
			ByteBuffer readBuf = selectBuf.asReadOnlyBuffer();
			readBuf.position(0);
			while(readBuf.position()<bound){
				readBuf.position((readBuf.position() + 4)); //skip the object id
				longnum = readBuf.getLong();
				sum += longnum;
			}
		}
		return sum;
	}
 
	public long select(byte[][] columns){
		long sum = 0;
		long longnum = 0;
        
        ByteBuffer [] selectBufs = new ByteBuffer [columns.length];
        int [] bounds = new int [columns.length];
        
        int index = 0;
        for(String key: colBufs.keySet()){
            String columnKey = (key.split(separator))[0];
            for(int i = 0; i < columns.length; i++){
                if(Arrays.equals(columnKey.getBytes(), columns[i])==true){
                    //System.out.println("select buffer"+key);
                    ByteBuffer sbuf = colBufs.get(key);
                    //record current write position
                    bounds[index]=sbuf.position();
                    //create a read buffer for this column
                    ByteBuffer rBuf = sbuf.asReadOnlyBuffer();
                    rBuf.position(0);
                    selectBufs[index] = rBuf;
                    index += 1;
                }
            }
        }
        
        while(selectBufs[0].position() < bounds[0]){
            selectBufs[0].position((selectBufs[0].position() + 4)); //skip the object id
            longnum = selectBufs[0].getLong();
            sum += longnum;
            for(int i = 1; i< index ; i++){
                //similar as getObjectFromLongBuf
                ByteBuffer rBuf = selectBufs[i];
                int wBound = bounds[i];
                //int cOid = 0;
                while(rBuf.position() < wBound){
                    rBuf.position((rBuf.position() + 4)); //skip the object id
                    longnum = rBuf.getLong();
                    sum += longnum;
                    
                    break;
                } // while rBuf
            }
            
        }
        
        
        return sum;
	}
 	/* Return the sum of specified columns if condition in where is met
	*  Assuming there's only one where column and there can be multiple select columns
	*/
	public long selectWhere(byte[][] selectCols, byte[] whereCol, int threshold){	
		long sum = 0;
		long longnum = 0;
		//get where buf
        String whereKey = new String(whereCol) + separator + "LONG";
        ByteBuffer whereBuf = colBufs.get(whereKey);
        
		if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey); 
            return -1;
        }

		ByteBuffer whereReadBuf = whereBuf.asReadOnlyBuffer();       
		whereReadBuf.position(0);
		int wbound = whereBuf.position();        

		ByteBuffer [] selectBufs = new ByteBuffer [selectCols.length];
        int [] bounds = new int [selectCols.length];
		int [] positions = new int [selectCols.length];        

        int index = 0;
        for(String key: colBufs.keySet()){
            String columnKey = (key.split(separator))[0];
            for(int i = 0; i < selectCols.length; i++){
                if(Arrays.equals(columnKey.getBytes(), selectCols[i])==true){
                    //System.out.println("select buffer"+key);
                    ByteBuffer sbuf = colBufs.get(key);
                    //record current write position
                    bounds[index]=sbuf.position();
                    //create a read buffer for this column
                    ByteBuffer rBuf = sbuf.asReadOnlyBuffer();
                    rBuf.position(0);
					positions[index] = 0;
                    selectBufs[index] = rBuf;
                    index += 1;
                }
            }
        }
        int rowSkipped = 0;

        while(whereReadBuf.position() < wbound){
			//boolean rowSelected = false;
            whereReadBuf.position((whereReadBuf.position() + 4)); //skip the object id
            longnum = whereReadBuf.getLong();
            
			if(longnum < threshold){
                for(int i = 0; i< index ; i++){
                    positions[i] += ((4+8)*rowSkipped + 4);
					selectBufs[i].position(positions[i]);
					longnum = selectBufs[i].getLong();
					positions[i] += 8;
					sum += longnum;
                }
                rowSkipped = 0;
			}
            else{
                //for(int i = 0; i< index ; i++){
				//	positions[i] = positions[i] + 4 + 8;
                //}
                rowSkipped++;
            }
        }
/*
        while(whereReadBuf.position() < wbound){
			boolean rowSelected = false;
            whereReadBuf.position((whereReadBuf.position() + 4)); //skip the object id
            longnum = whereReadBuf.getLong();
			if(longnum < threshold){
				rowSelected = true;
			}            

			for(int i = 0; i< index ; i++){
				if(rowSelected == true){
					positions[i] += 4;
					selectBufs[i].position(positions[i]);
					longnum = selectBufs[i].getLong();
					positions[i] += 8;
					sum += longnum;
				}
				else{
					positions[i] = positions[i] + 4 + 8;
				}
            }
        }*/
        return sum;
	}
    public long aggregateColumn(byte[] selectCol, byte[] whereCol, int threshold){
        long sum = 0;
        // for now, the where filed is LONG 
        String whereKey = new String(whereCol) + separator + "LONG";
        ByteBuffer whereBuf = colBufs.get(whereKey);
        //System.out.println("search column key"+whereKey); 
        String selectKey = new String(selectCol) + separator + "LONG";
        ByteBuffer selectBuf = colBufs.get(selectKey);

        //ByteBuffer [] selectBufs = new ByteBuffer [colBufs.size()-1];
        int [] bounds = new int [colBufs.size() - 1];

		if(selectBuf == null){
            System.out.println("Didn't find buffer key"+selectKey); 
            return -1;
        }
        if(whereBuf == null){
            System.out.println("Didn't find buffer key"+whereKey); 
            return -1;
        }
        ByteBuffer whereReadBuf = whereBuf.asReadOnlyBuffer();
		ByteBuffer selectReadBuf = selectBuf.asReadOnlyBuffer();

        int bound = whereBuf.position();
		int sbound = selectBuf.position();
        whereReadBuf.position(0);
		selectReadBuf.position(0); 
        int oid = 0;
        while(whereReadBuf.position()<bound){
            oid = whereReadBuf.getInt();
            long value = whereReadBuf.getLong();
            if(value <= threshold){
				int cOid = 0;
				while(selectReadBuf.position() < sbound){
					cOid = selectReadBuf.getInt();
					if(cOid > oid){
						//position back
						int newPosition = selectReadBuf.position()-Integer.SIZE/Byte.SIZE;
						selectReadBuf.position(newPosition);
						break;
					}
					else if(oid == cOid){
						long sValue = selectReadBuf.getLong();
						sum = sum + sValue;
					}
					else{
						int newPosition = selectReadBuf.position()+Long.SIZE/Byte.SIZE;
						selectReadBuf.position(newPosition);
					}
				}
			}
        }
        
        return sum;
    } 


    public static void main(String[] args) throws IOException{


        /* flatten the json file */ 
        //JsonReader reader = Json.createReader(new FileReader("testjson/test.json")); 
        JsonReader reader = Json.createReader(new FileReader("testjson/abc.json")); 
        JsonObject jsonob = reader.readObject();
        System.out.println(jsonob.toString());
        ColStore store= new ColStore(10*1000*1000);
        int objid = 1;
        store.insertObject(objid,jsonob,null);
        store.insertObject(2,jsonob,null);
        store.insertObject(3,jsonob,null);
        /* populate the table */

        System.out.println("get the result out \n");
        /* objid, keystr,valstr,valnum,valbool - 5 bytes */
        /* read it out */
        store.getObject(2);

        //aggregate scan
        String targetColumn = "CC";
        long sum = store.aggregate(targetColumn.getBytes(),10);
        System.out.println("Aggregate sum results :"+sum);

    }
}
