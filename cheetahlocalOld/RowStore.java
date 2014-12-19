/**
 * An row-based buffer store  
 * @author Alan Lu
 */
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Arrays;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.io.BufferedReader;

public class RowStore {
	private int max_buf_size;
	private ByteBuffer buffer; //save all of the contents
	private int  UNDEFINED= -1111111; //represent NULL  
    private byte [] UndefByte= new byte[1];
    private static String [] fields;
	private static String [] types;
    private static Hashtable <String, Long> tempBufLong = new Hashtable<String, Long>(); 
    private static Hashtable <String, Double> tempBufDouble = new Hashtable<String, Double>(); 
    String separator=":";
    /**
     * Creates a ring buffer
	 * initialize the buffer size 
     */
    public RowStore (int memory_size_in_bytes, String defFile)
    {
		int max_buf_size = memory_size_in_bytes; 

		// create a byte buffer 
		buffer = ByteBuffer.allocateDirect(max_buf_size);
		//buffer = new byte[max_buf_size];

        UndefByte[0] = -1;
        
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(defFile));
            String line  = bufferedReader.readLine();
			String[] temp = line.split("\\s+");
			fields = new String[temp.length];
			types = new String[temp.length];
			for(int i=0; i<temp.length; i++){
				String[] temp2 = temp[i].split(":");
				fields[i] = temp2[0];
				types[i] = temp2[1];
			}
			//fields = temp.split(":")[];
			//fields = line.split("\\s+"); //separted by any white space 
            bufferedReader.close();
        } catch (FileNotFoundException e){
                        System.err.println("FileNotFoundException:"+e.getMessage());
                        return ;
        } catch (IOException e){
                        System.err.println("IOException:"+e.getMessage());
                        return;
        }
	}
    
    public int getRow(int targetId)
	{
		ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        // start from the beginning 
		readBuf.position(0);
		int oid = 0;
		long longnum=0;
		double doublenum=0;

		while(readBuf.position()<bound){ 
            // scan the buffer 
			oid = readBuf.getInt();
			if(oid > targetId)
				break; /* found the target row - assume monotonous increase */
			if(oid == targetId)
				System.out.print("Row "+oid);
		
			for(int i = 0; i < fields.length; i++){
				//String [] parts = fields[i].split(separator);
				//String columnKey = parts[0];
	            String type = types[i];
	            
	            if(type.equals("STRING")){
	            }else if(type.equals("LONG")){
	            	longnum = readBuf.getLong();
	            	if(oid == targetId)
	    				System.out.print(" "+longnum);
	            }else if(type.equals("DOUBLE")){
	            	doublenum = readBuf.getDouble();
	            	if(oid == targetId)
	            		System.out.print(" "+doublenum);
	            }else if(type.equals("BOOL")){
	            }else{
	                System.out.println("Error: no such type in buf "+type);
	                return 1;
	            }
			}
			if(oid == targetId)
				System.out.println("");
		} /* end while */
        return 0;
	}
   
	public void insertObject(int objid, JsonValue tree, String key){
		tempBufLong.clear();
		tempBufDouble.clear();
		prepareRow(objid, tree, key);
		insertRow(objid);
	}
 
    public void prepareRow(int objid, JsonValue tree, String key){
        switch(tree.getValueType()){
		case OBJECT:
			JsonObject object = (JsonObject) tree;
			for(String name: object.keySet()){
				if(key!=null)
					prepareRow(objid,object.get(name),key+"."+name);
				else
					prepareRow(objid,object.get(name),name);
			}
            // check the size
            //if((objid % 10000) == 1) 
           //    System.out.println("Row id " + objid+ "buffer offset "+buffer.position());
			break;
		case ARRAY:
			JsonArray array = (JsonArray) tree;
			int index =0; 
			for (JsonValue val : array){
				prepareRow(objid,val,key+"["+index+"]");
				index += 1;
			}
			break;
		case STRING:
			JsonString st = (JsonString) tree;
			//saveStrValue(objid,key,st.getString());
			break;
		case NUMBER:
			JsonNumber num = (JsonNumber) tree;
            if(num.isIntegral()){
            	tempBufLong.put(key, num.longValue());
            }else{
            	tempBufDouble.put(key, num.doubleValue());
            }
			break;
		case TRUE:
		case FALSE:
			//saveBoolValue(objid,key,tree.getValueType().toString());
			break;
		case NULL:
			break;
        }
	}
    
	public void insertRow(int objid){
		//leave 1K as the warning threshold - don't insert after it 
	    if(buffer.position() > buffer.capacity() - 1000){
	    	//don't insert any more -- buffer almost full
	    	System.out.println("buffer is almost full. No inserts any more!");
	    	return;
	    }
	    //first put the objid
		buffer.putInt(objid);
		
		for(int i = 0; i < fields.length; i++){
			//String [] parts = fields[i].split(separator);
			String key = fields[i];
            String type = types[i];
            
            if(type.equals("STRING")){
            }
            else if(type.equals("LONG")){
    			if(tempBufLong.get(key) != null)
    				buffer.putLong(tempBufLong.get(key));
    			else
					buffer.putLong(UNDEFINED);
            }
            else if(type.equals("DOUBLE")){
            	if(tempBufDouble.get(key) != null)
            		buffer.putDouble(tempBufDouble.get(key));
    			else
					buffer.putLong(UNDEFINED);
            }
            else if(type.equals("BOOL")){
            }
            else{
                System.out.println("Error: no such type in buf "+type);
                return;
            }
		}
	}
/* Return the sum of specified columns*/
	public long select(byte[][] columns){	
		ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        readBuf.position(0);
		long sum = 0;
        long longnum = 0;
        
		int[] selectFields = new int[fields.length];
	
		for(int i = 0; i < fields.length; i++){
			selectFields[i] = 0;
			String columnKey = fields[i];
			for(int j = 0; j < columns.length; j++){
	 			if(Arrays.equals(columnKey.getBytes(), columns[j])==true){
					selectFields[i] = 1; 
				}
			}
		}
		int position = 0;
		while(readBuf.position()<bound){
			//skip an int for the objid, no need to read it
			position += 4;
			for(int i = 0 ; i < fields.length; i++){
				//should not read this field, skip a long
				if(selectFields[i] == 0){
					position += 8;
				}			
				else{
					readBuf.position(position);
					position += 8;
					longnum = readBuf.getLong();
					sum += longnum;
				}
			}
		}
		return sum;
	}
	/* Return the sum of specified columns*/
	public long select2(byte[][] columns){	
		ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        readBuf.position(0);
		long sum = 0;
        long longnum = 0;
        
		int[] selectFields = new int[fields.length];
	
		for(int i = 0; i < fields.length; i++){
			selectFields[i] = 0;
			String columnKey = fields[i];
			for(int j = 0; j < columns.length; j++){
	 			if(Arrays.equals(columnKey.getBytes(), columns[j])==true){
					selectFields[i] = 1; 
				}
			}
		}
		while(readBuf.position()<bound){
			readBuf.position((readBuf.position() + 4));
			for(int i = 0 ; i < fields.length; i++){
				//longnum = readBuf.getLong();
				if(selectFields[i] == 1){
					longnum = readBuf.getLong();
					sum += longnum;
				}
				else{
					int oldpos = readBuf.position();
					int newpos = oldpos + 8; //one long
					readBuf.position(newpos);
				}
			}
		}
		return sum;
	}
	/* Return the sum of specified columns if condition in where is met
	*  Assuming there's only one where column and there can be multiple select columns
	*/
	public long selectWhere(byte[][] selectCols, byte[] whereCol, int threshold){	
		ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        readBuf.position(0);
		long rowsum = 0;
		long sum = 0;
        long longnum = 0;
        int whereIndex = 0; //the column that check the selectivity

		int[] selectFields = new int[fields.length];
	
		for(int i = 0; i < fields.length; i++){
			selectFields[i] = 0;
			String columnKey = fields[i];
			for(int j = 0; j < selectCols.length; j++){
	 			if(Arrays.equals(columnKey.getBytes(), selectCols[j])==true){
					selectFields[i] = 1; 
				}
			}
			if(Arrays.equals(columnKey.getBytes(), whereCol)==true){
				whereIndex = i;
				selectFields[i] = 2;
			}
		}

		int position = 0;
		while(readBuf.position()<bound){
            //boolean rowSelected = false;
			//rowsum = 0;
            //skip an int for the objid, no need to read it
			position += 4;
            //skip to the wherefield
            readBuf.position(position+whereIndex*8);
            longnum = readBuf.getLong();
            if(longnum < threshold){
                //readBuf.position(position);
                for(int i = 0 ; i < fields.length; i++){
                    if(selectFields[i] == 1){
                        readBuf.position(position);
                        longnum = readBuf.getLong();
                        sum += longnum;
                    }
                    position += 8;
                }
            }
            else{
                position +=  8*(fields.length - whereIndex);
            }
            /*
			for(int i = 0 ; i < fields.length; i++){
				//should not read this field, skip a long
				//if(selectFields[i] == 0){
				//	position += 8;
				//}			
				if(selectFields[i] == 1){
					readBuf.position(position);
					//position += 8;
					longnum = readBuf.getLong();
					rowsum += longnum;
				}
				else if(selectFields[i] == 2){
					readBuf.position(position);
					//position += 8;
					longnum = readBuf.getLong();
					//if this row is selected
					if(longnum < threshold){
						rowSelected = true;
					}
					//if this row is not selectd, there's no need to scan the rest of the row, skip the rest of the row
					//else{
					//	position += 8*(fields.length-i-1);
					//	break;
					//}
				}
				position += 8;
			}
			
			if(rowSelected){
				sum += rowsum;
			}*/
		}
		return sum;
	}
	
    public long aggregateColumn(byte[] selectCol, byte[] whereCol, int threshold){
        /* assume we all have the same type , but need to check the column number*/
        ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        readBuf.position(0);
        long rowsum = 0;
		long sum = 0;
        long longnum = 0;
        int oid;
        int whereField = -1; //the column that check the selectivity
		int selectField = -1; //the column to be selected, if condition is met

        for(int i = 0 ; i < fields.length; i++){
			String columnKey = fields[i];
            if(Arrays.equals(columnKey.getBytes(), selectCol)==true){
                selectField = i;
                //break;
            }
			if(Arrays.equals(columnKey.getBytes(), whereCol)==true){
                whereField = i;
            }
        }

        if(whereField == -1){
            System.out.println("Error: no where field.");
            return -1;
        }
		if(selectField == -1){
            System.out.println("Error: no select field.");
            return -1;
        }

        while(readBuf.position()<bound){
            rowsum = 0;
			oid = readBuf.getInt();
            for(int i = 0 ; i < fields.length; i++){
				longnum = readBuf.getLong();
                if(i == whereField){
                    // check the selectivity
                    if(longnum>= threshold){ // not selected
                        int oldpos = readBuf.position();
                        int newpos= oldpos+(fields.length-i-1)*8; // one long
                        readBuf.position(newpos); //skip this row
                        rowsum = 0; //reset rowsum
                        break;
                    }
                }
				if(i == selectField){
                	rowsum += longnum;
				}
            }
			sum += rowsum;
        }
        return sum;
    }


    public long fastAggregate(byte[] colName,int threshold){
        /* assume we all have the same type , but need to check the column number*/
        ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        readBuf.position(0);
        long rowsum = 0;
        long longnum = 0;
        int oid;
        int colField = -1; //the column that check the selectivity
        for(int i = 0 ; i < fields.length; i++){
			String columnKey = fields[i];
            if(Arrays.equals(columnKey.getBytes(), colName)==true){
                colField = i;
                break;
            }
        }
        if(colField == -1){
            System.out.println("Error: no select field.");
            return -1;
        }


        while(readBuf.position()<bound){
            oid = readBuf.getInt();
            for(int i = 0 ; i < fields.length; i++){
                longnum = readBuf.getLong();
                if(i == colField){
                    // check the selectivity
                    if(longnum>= threshold){ // not selected
                        int oldpos = readBuf.position();
                        int newpos= oldpos+(fields.length-i-1)*8; // one long
                        readBuf.position(newpos); //skip this row
                        rowsum = 0; //reset rowsum
                        break;
                    }
                }
                rowsum += longnum;
            }
        }
        return rowsum;

    }

    public long fastAggregate1(byte[] colName,int threshold){
        /* assume we all have the same type */
        ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        readBuf.position(0);
        long rowsum = 0;
        long longnum = 0;
        int oid;
        //assume selectivity is 1 for now
        //simple sum all of them
        while(readBuf.position()<bound){
            oid = readBuf.getInt();
            for(int i = 0 ; i < fields.length; i++){
	       	    longnum = readBuf.getLong();
                rowsum += longnum;
            }
        }
        return rowsum;
            
 
    }


	public long aggregate(byte[] colName,int threshold){
    	ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position();
        // start from the beginning 
		readBuf.position(0);
        long rowsum = 0;
        long sum=0;
		long longnum=0;
		double doublenum=0;
		int oid = 1;
        boolean selectFlag = false; //indicate whether this row is selected or not, based on the condition
        //boolean fieldFlag = false; //indicate whether this field is the conditional check field or not
		String column = new String(colName);

        while(readBuf.position()<bound){ 
            // scan the buffer 
			oid = readBuf.getInt();

			for(int i = 0; i < fields.length; i++){
				String columnKey = fields[i];
				String type = types[i];
	
	            //if(Arrays.equals(columnKey.getBytes(), colName)==true){
	            //	fieldFlag = true;
	            //}else{
	            //	fieldFlag = false;
	            //}
 
	            if(type.equals("STRING")){
	            }else if(type.equals("LONG")){
	            	longnum = readBuf.getLong();
	            	//if(fieldFlag==true){
	                if(columnKey.equals(column)){  
						  //check selectivity 
	                    if(longnum <= threshold){
	                        selectFlag = true; 
	                    } 
	                }
	                rowsum += longnum;
	            }else if(type.equals("DOUBLE")){
	            	doublenum = readBuf.getDouble();
	            }else if(type.equals("BOOL")){
	            }else{
	                System.out.println("Error: no such type in buf "+type);
	                return 1;
	            }
			}
	
            if(selectFlag==true){
            	sum += rowsum;
              //  System.out.println("row sum value "+rowsum+" sum "+sum);
            }
                //System.out.println("reset rowsum,selectFlag");
                // reset row stats and selectFlag -- must happen before we check the key
            rowsum = 0;
            selectFlag = false;
		} /* end while */
        return sum;
    }
    
	public static void main(String[] args) throws IOException{
		JsonReader reader = Json.createReader(new FileReader("testjson/abcde2.json")); 

		//Assuming we know all the columns and data type in advance
		//fields = new String[] {"A:LONG","B:LONG","C:LONG","D:LONG","E:DOUBLE"};
		
		JsonObject jsonob = reader.readObject();
		System.out.println(jsonob.toString());
		RowStore store= new RowStore(100*1000*1000, "testjson/abcde2_definition");
		
		//insert objects
		for(int objid=1; objid<=10; objid++){
			store.insertObject(objid,jsonob,null);
		}

		System.out.println("get the result out \n");
		store.getRow(0);
		// aggregate scan   
        String targetColumn = "D";
        long sum = store.aggregate(targetColumn.getBytes(),500);
        System.out.println("Aggregate sum Results : "+sum);
        sum = store.fastAggregate(targetColumn.getBytes(),2);
        System.out.println("Aggregate sum Results : "+sum);
	}
}
