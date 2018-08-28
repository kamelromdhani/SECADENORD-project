package seca;
import java.text.ParseException;

import java.text.SimpleDateFormat;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Path("/seca")
public class Seca {
	Connection myConn = null;
	Statement myStmt = null;
	ResultSet myRs = null;
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/tableNiveau")
	public String tableNiveau() {
		JSONArray ja = new JSONArray();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// 1. Get a connection to database
			myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/secadenord", "root" , "");
			
			// 2. Create a statement
			myStmt = myConn.createStatement();
			
			// 3. Execute SQL query
			//in production replace query with : SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE FROM niveau where DATETRAJET in (select max(DATETRAJET) from niveau WHERE IDBOITIER IN(953136,953125,953134,953143,953146,953147,953149,953150,953152,953153) group by IDBOITIER)
			//or query=SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE FROM niveau where DATE(DATETRAJET)=DATE(NOW()) AND IDBOITIER IN(953136,953125,953134,953143,953146,953147,953149,953150,953152,953153) group by IDBOITIER)
			myRs = myStmt.executeQuery("SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE FROM niveau where DATE(DATETRAJET)='2018-03-22' AND IDBOITIER IN(953136,953125,953134,953143,953146,953147,953149,953150,953152,953153) group by IDBOITIER");
			
			// 4. Process the result set
			while (myRs.next()) {
				JSONObject Personobj = new JSONObject();
				Personobj.put("IDBOITIER", myRs.getString("IDBOITIER"));
				Personobj.put("DATETRAJET", myRs.getString("DATETRAJET"));
				Personobj.put("CARBURANT", myRs.getString("CARBURANT"));
				Personobj.put("TEMPERATURE", myRs.getString("TEMPERATURE"));
				ja.add(Personobj);

			}
			
			return ja.toString();
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
		finally {
			if (myRs != null) {
				try {
					myRs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myStmt != null) {
				try {
					myStmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myConn != null) {
				try {
					myConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return ja.toString();
		}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/tableDebit")
	public String tableDebit() {
		JSONArray ja = new JSONArray();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// 1. Get a connection to database
			myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/secadenord", "root" , "");
			
			// 2. Create a statement
			myStmt = myConn.createStatement();
			
			// 3. Execute SQL query
			//in production we may change the query to : SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE, AN3, AN4 FROM niveau where DATETRAJET in (select max(DATETRAJET) from niveau WHERE IDBOITIER IN(953145,953148,953151,953155,953157) group by IDBOITIER)
			//or SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE, AN3, AN4 FROM niveau where DATE(DATETRAJET) =DATE(NOW()) AND IDBOITIER IN (953145,953148,953151,953155,953157) goup by idboitier
			myRs = myStmt.executeQuery("SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE, AN3, AN4 FROM niveau where DATE(DATETRAJET) ='2018-03-22' AND IDBOITIER IN (953145,953148,953151,953155,953157) GROUP BY idboitier");
			
			// 4. Process the result set
			while (myRs.next()) {
				JSONObject Personobj = new JSONObject();
				Personobj.put("IDBOITIER", myRs.getString("IDBOITIER"));
				Personobj.put("DATETRAJET", myRs.getString("DATETRAJET"));
				Personobj.put("CARBURANT", myRs.getString("CARBURANT"));
				Personobj.put("TEMPERATURE", myRs.getString("TEMPERATURE"));
				Personobj.put("AN3", myRs.getString("AN3"));
				Personobj.put("AN4", myRs.getString("AN4"));
				ja.add(Personobj);

			}
			
			return ja.toString();
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
		finally {
			if (myRs != null) {
				try {
					myRs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myStmt != null) {
				try {
					myStmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myConn != null) {
				try {
					myConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return ja.toString();
		}
	
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/courbeNiveau/{IDBOITIER}/{DATEDEBUT}/{DATEFIN}")
	public String courbeNiveau(@PathParam("IDBOITIER") String IDBOITIER, @PathParam("DATEDEBUT") String DATEDEBUT, @PathParam("DATEFIN") String DATEFIN) {
		JSONArray ja = new JSONArray();
		JSONArray ja2 = new JSONArray();
		JSONObject obj = new JSONObject();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// 1. Get a connection to database
			myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/secadenord", "root" , "");
			
			// 2. Create a statement
			myStmt = myConn.createStatement();
			
			// 3. Execute SQL query
			myRs = myStmt.executeQuery("SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE FROM niveau where DATE(DATETRAJET) BETWEEN CAST('"+DATEDEBUT+"' AS DATE) AND CAST('"+DATEFIN+"' AS DATE) and idboitier="+IDBOITIER);
			
			// 4. Process the result set
			while (myRs.next()) {
				JSONObject courbeObj = new JSONObject();
				courbeObj.put("y", myRs.getDouble("CARBURANT"));
				courbeObj.put("x", myRs.getString("DATETRAJET"));
				
				JSONObject courbeObj2 = new JSONObject();
				courbeObj2.put("y", myRs.getDouble("TEMPERATURE"));
				courbeObj2.put("x", myRs.getString("DATETRAJET"));
				
				ja2.add(courbeObj2);
				ja.add(courbeObj);

			}
			
			obj.put("aval", ja);
			obj.put("amont", ja2);

			return obj.toString();
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
		finally {
			if (myRs != null) {
				try {
					myRs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myStmt != null) {
				try {
					myStmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myConn != null) {
				try {
					myConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return obj.toString();
		}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/courbeDebit/{IDBOITIER}/{DATEDEBUT}/{DATEFIN}")
	public String courbeDebit(@PathParam("IDBOITIER") String IDBOITIER, @PathParam("DATEDEBUT") String DATEDEBUT, @PathParam("DATEFIN") String DATEFIN) {
		JSONArray ja1 = new JSONArray();
		JSONArray ja2 = new JSONArray();
		JSONArray ja3 = new JSONArray();
		JSONArray ja4 = new JSONArray();
		JSONObject mainobj = new JSONObject();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// 1. Get a connection to database
			myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/secadenord", "root" , "");
			
			// 2. Create a statement
			myStmt = myConn.createStatement();
			
			// 3. Execute SQL query
			myRs = myStmt.executeQuery("SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE, AN3, AN4 FROM niveau where DATE(DATETRAJET) BETWEEN CAST('"+DATEDEBUT+"' AS DATE) AND CAST('"+DATEFIN+"' AS DATE) and idboitier="+IDBOITIER);
			
			// 4. Process the result set
			while (myRs.next()) {
				JSONObject Pointobj1 = new JSONObject();
				JSONObject Pointobj2 = new JSONObject();
				JSONObject Pointobj3 = new JSONObject();
				JSONObject Pointobj4 = new JSONObject();
				Pointobj1.put("x", myRs.getString("DATETRAJET"));
				Pointobj2.put("x", myRs.getString("DATETRAJET"));
				Pointobj3.put("x", myRs.getString("DATETRAJET"));
				Pointobj4.put("x", myRs.getString("DATETRAJET"));
				
				Pointobj1.put("y", myRs.getDouble("CARBURANT"));
				Pointobj2.put("y", myRs.getDouble("TEMPERATURE"));
				Pointobj3.put("y", myRs.getDouble("AN3"));
				Pointobj4.put("y", myRs.getDouble("AN4"));
				
				
				
				ja1.add(Pointobj1);
				ja2.add(Pointobj2);
				ja3.add(Pointobj3);
				ja4.add(Pointobj4);

			}
			mainobj.put("courb1", ja1);
			mainobj.put("courb2", ja2);
			mainobj.put("courb3", ja3);
			mainobj.put("courb4", ja4);
			return mainobj.toString();
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
		finally {
			if (myRs != null) {
				try {
					myRs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myStmt != null) {
				try {
					myStmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myConn != null) {
				try {
					myConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return mainobj.toString();
		}
		
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/tableTotal/{DateMesure}")
	public String tableTotal(@PathParam("DateMesure") String DateMesure) {
		
		Connection myConn = null;
		Statement myStmt = null;
		ResultSet myRs = null;
		JSONObject obj = new JSONObject();
		JSONArray ja = new JSONArray();
		String boitiers[]= {"953145","953148","953151","953155","953157"}; 
		Vector<String> dates = new Vector<String>(), entrees1 = new Vector<String>(), entrees2 = new Vector<String>(), entrees3 = new Vector<String>(), entrees4 = new Vector<String>();
		Vector<Double> total1 = new Vector<Double>(), total2 = new Vector<Double>(), total3 = new Vector<Double>(), total4 = new Vector<Double>();
		//String idboitier="9531456";
		int a=0;
		try {
			// 1. Get a connection to database
			Class.forName("com.mysql.jdbc.Driver");
			myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/secadenord", "root" , "");
			
			// 2. Create a statement
			myStmt = myConn.createStatement();
			
			// 3. Execute SQL query
			// in production change query to : SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE, AN3, AN4 FROM niveau where DATE(DATETRAJET)='"+DateMesure+"' and IDBOITIER IN(953145,953148,953151,953155,953157) ORDER BY IDBOITIER"
			myRs = myStmt.executeQuery("SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE, AN3, AN4 FROM niveau where DATE(DATETRAJET) ='2018-03-22' and IDBOITIER IN(953145,953148,953151,953155,953157) ORDER BY IDBOITIER");
			// 4. Process the result set
			
			for (int k=0; k<5;k++) {
				total1.add(k, 0.0);
				total2.add(k, 0.0);
				total3.add(k, 0.0);
				total4.add(k, 0.0);
				
			}
			for(int j=0; j<5;j++) {
				boolean verif=false;
				while (myRs.next()) {
					if(myRs.getString("IDBOITIER").equals(boitiers[j])) {
						verif=true;
						//System.out.println("i am inside first while \n");
						
						//System.out.println("dates.size()= "+dates.size()+"\n");
						
						dates.add(myRs.getString("DATETRAJET"));
						//System.out.println("dates.elementAt(0)= "+dates.elementAt(0)+"\n");
						entrees1.add(myRs.getString("CARBURANT"));
						//System.out.println("entrees1.elementAt(0)= "+entrees1.elementAt(0)+"\n");
						entrees2.add(myRs.getString("TEMPERATURE"));
						entrees3.add(myRs.getString("AN3"));
						entrees4.add(myRs.getString("AN4"));
					}
					else {
						myRs.previous();
						break;
					}
					
				}
				
				if(verif) {
					//System.out.println("dates.elementAt(0)= "+dates.elementAt(0)+" a= "+a+"\n");
					JSONObject boitierObj = new JSONObject();
					boitierObj.put("IDBOITIER", boitiers[j]);
					boitierObj.put("total1", total(dates,entrees1));
					boitierObj.put("total2", total(dates,entrees2));
					boitierObj.put("total3", total(dates,entrees3));
					boitierObj.put("total4", total(dates,entrees4));
					ja.add(boitierObj);
					a++;
				}
				else {
					a++;}
				
				dates.clear();
				entrees1.clear();
				entrees2.clear();
				entrees3.clear();
				entrees4.clear();
			}
			
			
			
			
			/*System.out.println("dates.size= "+total1.size()+"\n");
			System.out.println("dates.size= "+total2.size()+"\n");
			System.out.println("dates.size= "+total3.size()+"\n");
			System.out.println("dates.size= "+total4.size()+"\n");*/
			
			
			/*System.out.println("*******total1*****");
			for(int i=0; i< total1.size();i++) {
				
				//System.out.println(total1.get(i)+"\n");
				//System.out.println("*******total2*****");
				System.out.println(total3.get(i)+"\n");
				System.out.println("*******total3*****");
				System.out.println(total3.get(j)+"\n");
				System.out.println("*******total4*****");
				System.out.println(total4.get(j)+"\n");
			}*/
			
			return ja.toString();
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
		finally {
			if (myRs != null) {
				try {
					myRs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myStmt != null) {
				try {
					myStmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myConn != null) {
				try {
					myConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return ja.toString();
		}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/courbeTotal/{IDBOITIER}/{dateDebut}/{dateFin}")
	public String courbeTotal(@PathParam("dateDebut") String dateDebut, @PathParam("dateFin") String dateFin, @PathParam("IDBOITIER") String IDBOITIER) {
		
		Connection myConn = null;
		Statement myStmt = null;
		ResultSet myRs = null;
		JSONObject obj = new JSONObject();
		JSONArray labels = new JSONArray();
		String datetrajet= "" ;
		Vector<String> dates = new Vector<String>(), entrees1 = new Vector<String>(), entrees2 = new Vector<String>(), entrees3 = new Vector<String>(), entrees4 = new Vector<String>();
		Vector<Double> total1 = new Vector<Double>(), total2 = new Vector<Double>(), total3 = new Vector<Double>(), total4 = new Vector<Double>();
		//String idboitier="9531456";
		int a=0;
		try {
			// 1. Get a connection to database
			Class.forName("com.mysql.jdbc.Driver");
			myConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/secadenord", "root" , "");
			
			// 2. Create a statement
			myStmt = myConn.createStatement();
			
			// 3. Execute SQL query
			myRs = myStmt.executeQuery("SELECT IDBOITIER, DATETRAJET, CARBURANT, TEMPERATURE, AN3, AN4 FROM niveau where DATE(DATETRAJET) BETWEEN CAST('"+dateDebut+"' AS DATE) AND CAST('"+dateFin+"' AS DATE) and idboitier="+IDBOITIER+" ORDER BY datetrajet ASC");
			// 4. Process the result set
			
			
			while(myRs.next()) {
				String parts[]= myRs.getString("DATETRAJET").split(" ");
				datetrajet=parts[0];
				dates.add(myRs.getString("DATETRAJET"));
				//System.out.println("dates.elementAt(0)= "+dates.elementAt(0)+"\n");
				entrees1.add(myRs.getString("CARBURANT"));
				//System.out.println("entrees1.elementAt(0)= "+entrees1.elementAt(0)+"\n");
				entrees2.add(myRs.getString("TEMPERATURE"));
				entrees3.add(myRs.getString("AN3"));
				entrees4.add(myRs.getString("AN4"));
				break;
				
			}
			
			while(myRs.next()) {
				while(myRs.next() && datetrajet.equals(myRs.getString("DATETRAJET").split(" ")[0])) {
					dates.add(myRs.getString("DATETRAJET"));
					//System.out.println("dates.elementAt(0)= "+dates.elementAt(0)+"\n");
					entrees1.add(myRs.getString("CARBURANT"));
					//System.out.println("entrees1.elementAt(0)= "+entrees1.elementAt(0)+"\n");
					entrees2.add(myRs.getString("TEMPERATURE"));
					entrees3.add(myRs.getString("AN3"));
					entrees4.add(myRs.getString("AN4"));
				}
				System.out.println("datetrajet= "+datetrajet+"\n");
				if(!datetrajet.equals(dateFin)) {
					
					//System.out.println("i am inside if 1"); 
					
					labels.add(datetrajet);
					datetrajet=myRs.getString("DATETRAJET").split(" ")[0];
					total1.add(a, total(dates,entrees1));
					total2.add(a, total(dates,entrees2));
					total3.add(a, total(dates,entrees3));
					total4.add(a, total(dates,entrees4));
					a++;
					dates.clear();
					entrees1.clear();
					entrees2.clear();
					entrees3.clear();
					entrees4.clear();
					myRs.previous();
				}
				else {
					//System.out.println("i am inside if 2");
					labels.add(datetrajet);
					total1.add(a, total(dates,entrees1));
					total2.add(a, total(dates,entrees2));
					total3.add(a, total(dates,entrees3));
					total4.add(a, total(dates,entrees4));
					break;
				}
				
				
			}
			
			obj.put("labels", labels);
			obj.put("total1", total1);
			obj.put("total2", total2);
			obj.put("total3", total3);
			obj.put("total4", total4);
			return obj.toString();
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}
		finally {
			if (myRs != null) {
				try {
					myRs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myStmt != null) {
				try {
					myStmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (myConn != null) {
				try {
					myConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return obj.toString();
		}

	

	private static double total(Vector<String> dates, Vector<String> entrees) {
		double s=0.0;
	//	System.out.println("i am in total function!");
		//System.out.println("dates.size="+dates.size());
		HashMap<String, String> hmap = new HashMap<String, String>();
		for(int k=0;k<dates.size();k++) {
			hmap.put(dates.get(k), entrees.get(k));
		}
		Map<String, String> map = new TreeMap<String, String>(hmap); 
        Set set2 = map.entrySet();
        Iterator iterator2 = set2.iterator();
        
        String xOld = "";
        while(iterator2.hasNext()) {
        	Map.Entry me2 = (Map.Entry)iterator2.next();
        	 xOld = (String) me2.getKey();
        	break;
        }
        
        while(iterator2.hasNext()) {
        	Map.Entry me2 = (Map.Entry)iterator2.next();
        	Object xNew = me2.getKey();
             java.util.Date x1 = null;
             java.util.Date x2 = null;
				try {
					x1 = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss.SS").parse((String) xOld);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					x2 = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss.SS").parse((String) xNew);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				 
					double valeur = Double.parseDouble( ((String) me2.getValue()).replace(",",".") );
					s+=(x2.getTime()-x1.getTime())*valeur; 
					xOld=(String) xNew;
					//System.out.println("valeur= "+valeur+"\n");
					//System.out.println("(x1.getTime()-x2.getTime())= "+(x2.getTime()-x1.getTime())+"\n");
					//System.out.println("x1= "+x1+"\t x2= "+x2+"\n");
            // System.out.print(me2.getKey() + ": ");
             //System.out.println(me2.getValue());
        }
		return s;
	}



}
