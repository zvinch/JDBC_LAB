/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab4;


import java.io.BufferedReader;
import java.io.Console;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 *
 * @author mzvin
 */
public class Lab4 {

    private static String dbURL = "jdbc:derby://localhost:1527/lab4db;create=true;user=Mylab4;password=lab4";
    private static String users = "USERS";
    private static String registration = "REGISTRATION";
    private static String drivername = "org.apache.derby.jdbc.ClientDriver";
   
    private static Path pathtofile =  Paths.get("C:\\Users\\mzvin\\Projects\\UNI\\JavaEE\\lab4\\data.log");
    // jdbc Connection
    private static Connection conn = null;
    private static Statement stmt = null;
    private static DatabaseMetaData meta = null;
    private static SimpleDateFormat NormalDatetimeFormatter = new SimpleDateFormat("dd.MM.yyyy hh:mm:ss");
    private static SimpleDateFormat DBdatetimeFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    
    private static int maxID = 0;
    private static int maxCode = 0;
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
         createConnection();
         createTables();
            getTMAX();
         parsefile(pathtofile);
         
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
          System.out.format("Добрый день. Соединение, таблицы, и первичные записи созданы.%n");
          System.out.format("Доступные команды:%n");
          System.out.format(" protocopy - выбрать все регистрационные записи%n");
          System.out.format(" usersBefo - пользователи входившие не ранее даты%n");
          System.out.format(" usersMult - пользовтели с множеством ролей %n");
          System.out.format(" dayMulti - дни в которые входили более одного пользователя%n");
          System.out.format(" exit - выход%n");
          System.out.format(" help - список команд%n");
         boolean working =true;
        
         
         String comand;
         while(working){
             try{
         System.out.format("Введите команду: %n");
         comand = br.readLine();
         if(comand.equals("protocopy")){
              System.out.format("%n");
             GetExactProtocolCopy();
             System.out.format("%n");
             
         }else if(comand.equals("usersBefo")){
             System.out.format("Введите дату в формате dd.MM.yyyy hh:mm:ss: %n");
             comand = br.readLine();
             GetAllUsersWHoEnteredBefore(NormalDatetimeFormatter.parse(comand));
             
         }else if(comand.equals("usersMult")){
             System.out.format("%n");
             AllUsersWhoHaveMultipleRoles();
             System.out.format("%n");
             
         }else if(comand.equals("dayMulti")){
             System.out.format("%n");
             ALLDaysWhichHadMorethenTwoUsers();
             System.out.format("%n");
         }
         else if(comand.equals("exit")){
             System.out.format("Хорошего дня! %n");
             working = false;
         }
         else if(comand.equals("help")){
          System.out.format("Доступные команды:%n");
          System.out.format(" protocopy - выбрать все регистрационные записи%n");
          System.out.format(" usersBefo - пользователи входившие не ранее даты%n");
          System.out.format(" usersMult - пользовтели с множеством ролей %n");
          System.out.format(" dayMulti - дни в которые входили более одного пользователя%n");
          System.out.format(" exit - выход%n");
          System.out.format(" help - список команд%n");
         }
         comand="";
             }catch(Exception ex){
                 ex.printStackTrace();
             }
         }
         shutdown();
    }
    
    //выбрать все регистрационные записи (запрос должен точно воспроизвести исходный файл протокола);
    private static ResultSet GetExactProtocolCopy(){
            try
            {
                StringBuilder strb= new StringBuilder();
                
                strb.append(" SELECT "+users+".id, "+users+".login,  "
                    + " CAST("+registration+".date AS DATE), "
                    + " CAST("+registration+".date AS TIME) ")
                    .append(" FROM "+users)
                     .append(" INNER JOIN "+registration+" ")
                    .append(" ON "+users+".id = "+registration+".id ");
            
                
                stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(strb.toString());
                PrintResultSet(rs);
                stmt.close();
                return rs;
                
            }catch(Exception ex){
                ex.printStackTrace();
            }
        return null;
    }
    
    //выбрать всех пользователей, которые регистрировались (входили в систему) не ранее заданной даты (дата определяется как параметр программы или вводится пользователем в диалоговом окне);
    private static ResultSet GetAllUsersWHoEnteredBefore(Date d){
            try
            {
                StringBuilder strb= new StringBuilder();
                
                strb.append("SELECT "+users+".login, "+registration+".date "
                    + "FROM "+users+" "
                    + "INNER JOIN "+registration+" "
                    + "ON "+users+".id = "+registration+".id "
                    + "WHERE "+registration+".date < CAST('"
                    + DBdatetimeFormatter.format(d)
                    +"' AS timestamp)");
            
                
                stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(strb.toString());
                PrintResultSet(rs);
                stmt.close();
                return rs;
                
            }catch(Exception ex){
                ex.printStackTrace();
            }
        return null;
    }
    
    //выбрать всех пользователей, которые регистрировались с разными ролями (т.е. не мене чем с двумя);
    private static ResultSet AllUsersWhoHaveMultipleRoles(){
        try
            {
                StringBuilder strb= new StringBuilder();
                
                strb.append("SELECT "+users+".login, stub.role FROM "+users+""
                     + " INNER JOIN "
                     + "(SELECT DISTINCT stub2.id AS id, stub3.role AS role"
                     + " FROM "+registration+" stub2"
                     + " INNER JOIN "+registration+" stub3 ON stub2.id = stub3.id"
                     + " WHERE (stub2.role != stub3.role)) stub"
                     + " ON ("+users+".id = stub.id)");
            
                
                stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(strb.toString());
                PrintResultSet(rs);
                stmt.close();
                return rs;
                
            }catch(Exception ex){
                ex.printStackTrace();
            }
        return null;
    }
    //выбрать все даты, в которые регистрировалось не менее двух пользователей.
     private static ResultSet ALLDaysWhichHadMorethenTwoUsers(){
         try
            {
                StringBuilder strb= new StringBuilder();
                
                strb.append("SELECT DISTINCT DATE(TIMESTAMP(stub.date)) FROM "+registration+" stub "
                    + "INNER JOIN "+registration+" stub2 "
                    + "ON (DATE(TIMESTAMP(stub.date)) = DATE(TIMESTAMP(stub2.date))) "
                    + "WHERE (stub.id != stub2.id)");
            
                
                stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(strb.toString());
                PrintResultSet(rs);
                stmt.close();
                return rs;
                
            }catch(Exception ex){
                ex.printStackTrace();
            }
        return null;
     }
    
     private static void PrintResultSet(ResultSet rs){
         try{
             
             ResultSetMetaData rsmd = rs.getMetaData();
         while (rs.next()) {
                for (int i = 1; i < rsmd.getColumnCount()+1; i++) {
                    System.out.format(rs.getString(i) + " ");
                }
                System.out.format("%n");
            }
         } catch(Exception ex){
             ex.printStackTrace();
         }
     }
     
     
     private static void createConnection()
    {
        try
        {
            Class.forName(drivername).newInstance();
            //Get a connection
            conn = DriverManager.getConnection(dbURL);
            meta = conn.getMetaData();
        }
        catch (Exception except)
        {
            except.printStackTrace();
        }
    }
     
     private static void getTMAX(){
           try
        {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT MAX(ID) "
                         +"FROM "+users
                );
            if(rs.next()){
                
                maxID = rs.getInt(1); //WTF
            }
            
            rs = stmt.executeQuery("SELECT MAX(CODE) "
                         +"FROM "+registration
                );
            if(rs.next()){
                maxCode = rs.getInt(1);
            }
            
             stmt.close();
        }
        catch (Exception except)
        {
            except.printStackTrace();
        }
     }
     
     private static void createTables(){
         
         
         try
        {
            ResultSet res = meta.getTables(null, null, "%", null);
            ArrayList<String> tablenames = new ArrayList<String>();
            while(res.next()){
                tablenames.add(res.getString("TABLE_NAME"));
            }
            if(!tablenames.contains(users)){
                stmt = conn.createStatement();
                stmt.execute("CREATE TABLE "+ users+
                            " ( id INT,"
                               + " Login VARCHAR(8) UNIQUE ,"
                                                +" PRIMARY KEY (id) ) "
                );
                stmt.close();
            }
            if(!tablenames.contains(registration)){
                         stmt = conn.createStatement();
            stmt.execute("CREATE TABLE "+ registration+
                        " (code INT, id INT,"
                           +"role VARCHAR(16),"
                           + " date TIMESTAMP ,"
                           +" PRIMARY KEY (code), "
                           +" FOREIGN KEY (id) REFERENCES "+users+"(id) )"
        );
            stmt.close();
            }
        }
        catch (SQLException sqlExcept)
        {
            sqlExcept.printStackTrace();
        }
     }
     
     private static void insertUniqueToUsers(String[] newlgns){
         try
        {
            stmt = conn.createStatement();
            
            ResultSet rs = stmt.executeQuery("SELECT LOGIN "
                         +"FROM "+users
                );
            
            Set<String> existing = new HashSet<>();
            while(rs.next()){
                existing.add(rs.getString(1));
            }
          
            stmt.close();
             
            
            Set<String> newcomming;
            newcomming = new HashSet<>(Arrays.asList(newlgns));
             
            newcomming.removeAll(existing);
            if(!newcomming.isEmpty()){
                
            
            StringBuilder strb= new StringBuilder();
            strb.append("INSERT INTO " + users+ " (id, login)");
            strb.append(" VALUES ");
                for (Iterator<String> iterator = newcomming.iterator(); iterator.hasNext();) {
                    strb.append("(").append(Integer.toString(++maxID)).append(" , '").append(iterator.next()).append("' )");
                    if(iterator.hasNext()){
                        strb.append(",");
                    }
                }
            //strb.append(";");
            
            
            
            stmt = conn.createStatement();
            stmt.execute(strb.toString());
             stmt.close();
            }
            
        }
        catch (Exception except)
        {
            except.printStackTrace();
        }
     }
     
     private static void insertToRegistration(String[] newlgns, String[] roles, String[] dates, String[] times ){
         try
        {
            stmt = conn.createStatement();
            
            
            
            StringBuilder strb= new StringBuilder();
            
            strb.append("INSERT INTO Registration (code, id, role, date)")
                        .append(" VALUES ");
            for(int i=0;i<newlgns.length; i++){
                        strb.append("("+ ++maxCode + ", (SELECT id FROM Users ")
                        .append("WHERE login='"+newlgns[i]+"'), '")
                        .append(roles[i])
                        .append("',");
                        
                        Date d = NormalDatetimeFormatter.parse(dates[i]+" "+times[i]);
                        
                       strb.append("CAST('" +DBdatetimeFormatter.format(d) +"' AS TIMESTAMP))");
                        if(i<newlgns.length-1){
                            strb.append(",");
                        }
            }
            
             stmt.execute(strb.toString());
             stmt.close();
        }
        catch (Exception except)
        {
            except.printStackTrace();
        }
     }
      
      private static void parsefile(Path p){
          String line;
    try {
        InputStream fis = new FileInputStream(p.toFile());
        InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
        BufferedReader br = new BufferedReader(isr);
    
        List<String> Logins = new ArrayList<String>();
        List<String> Positions = new ArrayList<String>();
        List<String> dates = new ArrayList<String>();
        List<String> times = new ArrayList<String>();
    while ((line = br.readLine()) != null) {
       String[] cartege = line.split(" ");
       
       if(cartege.length == 4){
           Logins.add(cartege[0]);
           Positions.add(cartege[1]);
           dates.add(cartege[2]);
           times.add(cartege[3]);
       } else{
           System.out.println("Check your file for fromat violations");
       }
    }
    insertUniqueToUsers(Logins.toArray(new String[0]));
    insertToRegistration(Logins.toArray(new String[0]),
                         Positions.toArray(new String[0]),
                         dates.toArray(new String[0]),
                         times.toArray(new String[0]));
    
    }catch(IOException e){
        
    }
  }
    
    private static void shutdown()
    {
        try
        {
            if (stmt != null)
            {
                stmt.close();
            }
            if (conn != null)
            {
                DriverManager.getConnection(dbURL + ";shutdown=true");
                conn.close();
            }           
        }
        catch (SQLException sqlExcept)
        {
            
        }

    }
    
    
}
