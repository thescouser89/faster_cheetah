import java.util.HashMap;
import java.util.Hashtable;
import java.io.StringReader;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;

public class QueryParallel extends Query implements Runnable { 

    //public int type; //0:empty 1:select only 2: select where
    //public String [] parameters;
    private String queryString;  //query to run
    private StoreEngine store;   //instance of store engine
    private long runTime;
    private String briefStatsString;
                   
    //debug
    private long queryId;
    
    private static PrintQueue printerQueue = null;//initialized by SimpleQueryExecutor
    
    QueryParallel(long queryId, StoreEngine store, String queryString){
        this.type = 0;
        this.queryId = queryId;
        this.store = store;
        this.queryString = queryString;
    }
    
    
    public static void setPrinterQueue( PrintQueue toSet ) {
        printerQueue = toSet;
    }
    
    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }
    public String getQueryString() { 
        return this.queryString;
    }
    
    public Long getRunTime() {
        return runTime;
    }
    
    public long getQueryId() {
        return this.queryId;
    }
    
    public void printBriefStats() {
        ResultBriefStats rbs = store.getBriefStatsForQuery(queryId);
        rbs.printBriefStats();
    }
    
    public void printBriefStatsString() { 
        System.out.print(briefStatsString);
    }
    
    //COPIED FROM SIMPLEQUERYEXECUTOR
    public void parse()
    {
        String queryString = this.queryString;
        //Query query = new Query();
        this.type = 0; // init to be empty query
        
        //queryString = queryString.toLowerCase(); - delete - avoid lower string value 
        if(queryString.endsWith(";")){
            queryString = queryString.substring(0,queryString.length()-1);
        }
        if(queryString.startsWith("SELECT") == true){
            if(queryString.contains("COUNT(*)" ) == true){
                // aggregation 
                if(queryString.contains("WHERE")==false){
                    this.type = 11; //select only aggregation
                }else{
                    //check where clause
                    if(queryString.contains("GROUP BY") == true){
                        if(queryString.contains("BETWEEN") &&  queryString.contains("AND")){
                            this.type = 13; //select where range and group by 
                            String [] tokens1 = queryString.split("SELECT")[1].trim().split("GROUP BY");
                            String whereString = tokens1[0].trim(); 
                            this.parameters = new String [5];
                            this.parameters[4] = tokens1[1].trim(); //group by column
                            String [] tokens=whereString.split("WHERE",2);
                            this.parameters[0] = tokens[0].trim() ; //select columns
                            String [] whereTokens = tokens[1].split("BETWEEN",2);
                            this.parameters[1] = whereTokens[0].trim(); // where columns
                            String [] rangeTokens = whereTokens[1].split("AND",2);
                            this.parameters[2] = rangeTokens[0].trim(); // small value
                            this.parameters[3] = rangeTokens[1].trim(); // large value
                    //System.out.println("select "+this.parameters[0]+" where "+this.parameters[1]+" range "+this.parameters[2]+ " "+this.parameters[3]+" group by "+this.parameters[4]);
                        }

                    }
                }

            } // end if aggregation query
            else 
            if(queryString.contains("WHERE") == false){
                this.type = 1; //select only
                String [] tokens = queryString.split("SELECT",2);
                this.parameters = new String[1];
                this.parameters[0] = tokens[1].trim(); 
                //System.out.println("select "+this.parameters[0]);
            }else{
                // check equal where or range where 
                // we need to write a better parser
                // we can only handle one single where column for now
                // select where ANY -- put it before select where
               if(queryString.contains("=") && queryString.contains("ANY")){
                    this.type = 4; //select where value = ANY xx
                    String whereString = queryString.split("SELECT")[1].trim();
                    String [] tokens=whereString.split("WHERE",2);
                    this.parameters = new String [4];
                    this.parameters[0] = tokens[0].trim() ; //select columns
                    String [] valueTokens = tokens[1].split("=",2);
                    this.parameters[2] = valueTokens[0].trim().replaceAll("^\"|\"$",""); //trim " from  where value
                    String [] whereTokens = valueTokens[1].trim().split("ANY",2); 
                    this.parameters[1] = whereTokens[1].trim(); // where columns
                    //System.out.println("SELECT "+this.parameters[0]+" WHERE "+this.parameters[2]+" = ANY "+this.parameters[1]);
                }
                else if(queryString.contains("=") == true){
                    this.type = 2; //select where equal
                    String whereString = queryString.split("SELECT")[1].trim();
                    String [] tokens=whereString.split("WHERE",2);
                    this.parameters = new String [3];
                    this.parameters[0] = tokens[0].trim() ; //select columns
                    String [] whereTokens = tokens[1].split("=",2);
                    this.parameters[1] = whereTokens[0].trim(); // where columns
                    // remove begining and end quotes from the value  
                    this.parameters[2] = whereTokens[1].trim().replaceAll("^\"|\"$",""); // trim " from where value
                    
                    //System.out.println("SELECT "+this.parameters[0]+" WHERE "+this.parameters[1]+" = "+this.parameters[2]);
                }
                else if(queryString.contains("BETWEEN") &&  queryString.contains("AND")){ 
                    this.type = 3; // select where range query
                    String whereString = queryString.split("SELECT")[1].trim();
                    String [] tokens=whereString.split("WHERE",2);
                    this.parameters = new String [4];
                    this.parameters[0] = tokens[0].trim() ; //select columns
                    String [] whereTokens = tokens[1].split("BETWEEN",2);
                    this.parameters[1] = whereTokens[0].trim(); // where columns
                    String [] rangeTokens = whereTokens[1].split("AND",2);
                    this.parameters[2] = rangeTokens[0].trim(); // small value
                    this.parameters[3] = rangeTokens[1].trim(); // large value
                    //System.out.println("SELECT "+this.parameters[0]+" WHERE "+this.parameters[1]+" range "+this.parameters[2]+ " "+this.parameters[3]);
                }
                else{
                    System.out.println("Unknown select where query "+queryString);
                }
            }//end of if
        }else if(queryString.startsWith("aggregate")==true){
            this.type = 10; //aggregate
            String [] tokens = queryString.split("aggregate");
            this.parameters = new String[1];
            this.parameters[0] = tokens[1].trim(); 
            System.out.println("aggregate "+this.parameters[0]);
		}else if (queryString.startsWith("INSERT")==true){
			this.type = 50; // insert			
            String insertObject = queryString.split("INSERT")[1].trim();
            this.parameters = new String[1];
			this.parameters[0] = insertObject;
            /*
			 * System.out.println("Insert");	
             */
        }else{
            System.out.println("Unknown query "+queryString);
        }   
    }
    
    //COPIED FROM SIMPLEQUERYEXECUTOR
    public String execute()
    {
        QueryParallel query = this;
        String results = "";

        //added by Alan to test SELECT all sparse fields
        if(query.parameters[0].equals("sparse_*")){
            StringBuffer strBuf = new StringBuffer();
            //String[] newPara = new String[1000];
            for(int i = 0; i < 1000; i++){
                strBuf.append( "sparse_" + String.format("%03d", i) );
                if(i != 999)
                    strBuf.append(",");
            }
            query.parameters[0] = strBuf.toString();
        }
        //System.out.println("Type: " + query.type);
        //System.out.println("execute ");
        switch (query.type){
            case 1: //select only clause
                String[] selectColumns = query.parameters[0].split(",");
                byte[][] columns = new byte[selectColumns.length][];
                for(int i=0; i<selectColumns.length; i++){
                    String column = selectColumns[i].trim();
                    //System.out.println("select only query: "+column);
                    columns[i] = column.getBytes();
                }
                //long start = System.currentTimeMillis();
                //HashMap<Integer, HashMap<String, String>> resultSet = store.select(columns);
                store.select(columns);
                //store.printBriefStats();
                //long end = System.currentTimeMillis();
                //System.out.print((end - start)+" ");
                //print_results(resultSet);
                break;
                
            case 2: //select where  A = "value"
                selectColumns = query.parameters[0].split(",");
                byte[][] sColumns = new byte[selectColumns.length][];
                for(int i=0; i<selectColumns.length; i++){
                    String column = selectColumns[i].trim();
                    //System.out.println("select where  = query: "+column);
                    sColumns[i] = column.getBytes();
                }
                // single where column
                byte [] wColumn = query.parameters[1].getBytes();
                byte [] value = query.parameters[2].getBytes();
                //start = System.currentTimeMillis();
                HashMap<Integer, HashMap<String, String>> resultSet = store.selectWhereSingle(sColumns, wColumn,"=", value);
                //end = System.currentTimeMillis();
                //System.out.print((end - start)+" ");
                //print_results(resultSet);
                break;
                
            case 3: //select where range
                selectColumns = query.parameters[0].split(",");
                sColumns = new byte[selectColumns.length][];
                for(int i=0; i<selectColumns.length; i++){
                    String column = selectColumns[i].trim();
                    //System.out.println("select where range query: "+column);
                    sColumns[i] = column.getBytes();
                }
                // single where column
                wColumn = query.parameters[1].getBytes();
                long small_value = Long.parseLong(query.parameters[2]);
                long large_value = Long.parseLong(query.parameters[3]);
                //long start = System.currentTimeMillis();
                //resultSet = store.selectRange(sColumns, wColumn, small_value,large_value);
                store.selectRange(sColumns, wColumn, small_value,large_value);
                //store.printBriefStats();
                //long end = System.currentTimeMillis();
                //System.out.print((end - start)+" ");
                //print_results(resultSet);
                break;
                
            case 4: //select where any
                selectColumns = query.parameters[0].split(",");
                sColumns = new byte[selectColumns.length][];
                for(int i=0; i<selectColumns.length; i++){
                    String column = selectColumns[i].trim();
                    //System.out.println("select where any query: "+column);
                    sColumns[i] = column.getBytes();
                }
                // single where column
                wColumn = query.parameters[1].getBytes();
                value = query.parameters[2].getBytes();
                //start = System.currentTimeMillis();
                resultSet = store.selectWhereAny(sColumns, wColumn,"=", value);
                //end = System.currentTimeMillis();
                //System.out.print((end - start)+" ");
                //print_results(resultSet);
                break;
            case 10://aggregate
                //only one column
                String colName = query.parameters[0];
                int threshold = 999999999; //very large number -- selectivity is 1
                results=String.valueOf(store.aggregate(colName.getBytes(),1000000)); 
                //System.out.println("Agg results:"+results);
                break;
            case 13: //aggregation where range , group by
                selectColumns = query.parameters[0].split(",");
                sColumns = new byte[selectColumns.length][];
                for(int i=0; i<selectColumns.length; i++){
                    String column = selectColumns[i].trim();
                    //System.out.println("select where range query: "+column);
                    sColumns[i] = column.getBytes();
                }
                // single where column
                wColumn = query.parameters[1].getBytes();
                small_value = Long.parseLong(query.parameters[2]);
                large_value = Long.parseLong(query.parameters[3]);
                byte[] gColumn = query.parameters[4].getBytes();
                Hashtable <String,Integer> aggResultSet = store.aggregateRangeGroupBy(sColumns, wColumn, small_value,large_value,gColumn);
                //print_agg_results(aggResultSet);//copied from SimpleQueryExecutor but fct doesn't do anything
                break;
			case 50: // insert single object from json string
				JsonReader reader = Json.createReader(new StringReader(query.parameters[0]));
				JsonObject jsonob = reader.readObject();
				store.insertObject(store.objCounter.getAndIncrement(),jsonob,null);
				reader.close();				
				break;
            default:
                System.out.println("Executor: unknown query type!");
        }//end of switch
        return results;
    }
    
    
    public void run() {
        this.parse();
        if(this.type > 0) {
            Thread.currentThread().setName(String.valueOf(queryId));
            long start = System.currentTimeMillis();
            this.execute();
            long end = System.currentTimeMillis();
            this.runTime = end - start;
            //print stuff here?
            
            briefStatsString = store.getBriefStatsString();
            
            //printerQueue.printerQueue.add(this);
        }
    }
}
