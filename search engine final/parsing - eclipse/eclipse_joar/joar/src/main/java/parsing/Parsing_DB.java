package parsing;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Parsing_DB {

	  private String errorMessages = "";
	  private Connection con = null;
	  private PreparedStatement stmt = null, stmt2 = null, stmt3 = null, stmt4 = null, stmt5 = null;
	  private ResultSet rs = null;

		private final String importSiteQuery = "INSERT INTO joar_sites (site) VALUES (?);";
		private final String selectSiteQuery = "SELECT * FROM joar_sites;";
		private final String updateSiteQuery = "UPDATE joar_sites SET title = ?, description = ? WHERE site=?;";

		private final String selectSitewordQuery = "SELECT * FROM joar_siteword WHERE site=? and word=?;";
		private final String importSitewordQuery = "INSERT INTO joar_siteword (site, word, frequency) VALUES (?,?,?);";
		private final String updateSitewordQuery = "UPDATE joar_siteword SET frequency = frequency + 1 WHERE site=? and word=?;";

		private final String selectWord2Query = "SELECT * FROM joar_word WHERE word=?;";
		private final String importWordQuery = "INSERT INTO joar_word (word, frequency) VALUES (?,?);";
		private final String updateWordQuery = "UPDATE joar_word SET frequency = frequency + 1 WHERE word=?;";

		private final String updateKeywordQuery = "UPDATE joar_siteword SET keyword = 1 WHERE site=? and word=?;";

	  public String getErrorMessages() {
	  	return errorMessages;
  	  }

	  public void open() throws SQLException {
	    try {
	      // for JDBC driver to connect to mysql, the .newInstance() method
	      // can be ommited
	      Class.forName("com.mysql.jdbc.Driver").newInstance();
	    } catch (Exception e1) {
	      errorMessages = "MySQL Driver error: <br>" + e1.getMessage();
	      throw new SQLException(errorMessages);
	    }

	    try {
	      con = DriverManager.getConnection(
	          "jdbc:mysql://195.251.249.131:3306/ismgroup77?characterEncoding=UTF-8",
	          "ismgroup77", "rx673g");
	    } catch (Exception e2) {
	      errorMessages = "Could not establish connection with the Database Server: <br>"
	          + e2.getMessage();
	      con = null;
	      throw new SQLException(errorMessages);
	    }

	  }

	  /**
	   * Ends the connection with the database Server. Closes all Statements and
	   * ResultSets. Finally, closes the connection with the Database Server.
	   *
	   * @throws SQLException
	   *             (with the appropriate message) if any error occured.
	   */
	  public void close() throws SQLException {
	    try {

	      if (stmt != null)
	        stmt.close();

				if (stmt2 != null)
					stmt2.close();

				if (stmt3 != null)
					stmt3.close();

				if (stmt4 != null)
					stmt4.close();

				if (stmt5 != null)
					stmt5.close();

	      if (rs != null)
	        rs.close();

	      if (con != null)
	        con.close();

	    } catch (Exception e3) {
	      errorMessages = "Could not close connection with the Database Server: <br>"
	          + e3.getMessage();
	      throw new SQLException(errorMessages);
	    }
 	 }

	 /**
	 *H methodos returnAllSites epistrefei ola ta sites tis basis, pio sugkekrimena epistrefei mia lista tupou Sites
	 *apo to konstraktora pou epistrefei mono to url site
	 */

 public List<Sites> returnAllSites() throws Exception {

	 if (con == null) {
		 errorMessages = "You must establish a connection first!";
		 throw new SQLException(errorMessages);
	 }

	 try {
		 List<Sites> list = new ArrayList<Sites>();

		 stmt = con.prepareStatement(selectSiteQuery);
		 // execute query
		 rs = stmt.executeQuery();
		 while (rs.next()) {
			 Sites sites = new Sites(rs.getString("site"));
			 list.add(sites);
		 }

		 rs.close();
		 stmt.close();

		return list;
	 } catch (Exception e) {
	throw new Exception("Error: " + e.getMessage());
}
}

	/**
	*H methodos importSiteword eite eisagei ston pinaka siteword (pou periexei tis lexeis tou sigkekrimenou site pou exoun parsariste) tis basis to site kai ti lexi,
	*me frequency = 1, afou i lexi brethike mia fora
	*eite eite eisagei ston pinaka siteword tis basis to site kai to keyword tou site, me frequency = 0, afou mporei kai na min uparxei san lexi sto html to keword
	*/

 public void importSiteword(String site, String word, int frequency) throws SQLException {

 	 if (con == null) {
 		 errorMessages = "You must establish a connection first!";
 		 throw new SQLException(errorMessages);
 	 }

 	 try {
 		 stmt3 = con.prepareStatement(importSitewordQuery);
 		 // replacing the first ? with am, the second ? with
 		 // name, the third ? with surname and the fourth ? with ip.
 		 stmt3.setString(1, site);
 		 stmt3.setString(2, word);
		 stmt3.setInt(3, frequency);
 		 // execute query
 		 stmt3.executeUpdate();
 		 stmt3.close();

 	 } catch (Exception e4) {
 		 errorMessages = "Error while inserting student to the database: <br>"
 				 + e4.getMessage();
 		 throw new SQLException(errorMessages);
 	 }
  }

	/**
	*H methodos importWord eisagei ston pinaka word (pou periexei oles tis lexeis olon ton site pou exoun parsariste),
	*me frequency = 1, afou i lexi brethike mia fora
	*/

	public void importWord(String word, int frequency) throws SQLException {

		if (con == null) {
			errorMessages = "You must establish a connection first!";
			throw new SQLException(errorMessages);
		}

		try {
			stmt3 = con.prepareStatement(importWordQuery);

			stmt3.setString(1, word);
			stmt3.setInt(2, frequency);
			// execute query
			stmt3.executeUpdate();
			stmt3.close();

		} catch (Exception e4) {
			errorMessages = "Error while inserting student to the database: <br>"
					+ e4.getMessage();
			throw new SQLException(errorMessages);
		}
	 }

	 /**H methodos importSites exei dothei kai xrisimopoieitai apo tous crawlers
	 *gia na eisagoun ta sites pou briskoun ston pinaka sites tis basis
	 */

	public void importSites(String site) throws SQLException {

		if (con == null) {
			errorMessages = "You must establish a connection first!";
			throw new SQLException(errorMessages);
		}

		try {
			stmt3 = con.prepareStatement(importSiteQuery);
			// replacing the first ? with am, the second ? with
			// name, the third ? with surname and the fourth ? with ip.
			stmt3.setString(1, site);
			// execute query
			stmt3.executeUpdate();
			stmt3.close();

		} catch (Exception e4) {
			errorMessages = "Error while inserting sites to the database: <br>"
					+ e4.getMessage();
			throw new SQLException(errorMessages);
		}
	 }

	 /**
	 *H methodos validSiteword elegxei eite an to site me ti lexi uparxoun ston pinaka siteword tis basis
	 *an to site me to keyword uparxoun ston pinaka siteword tis basis
	 */

	public boolean validSiteword (String site, String word) throws Exception {

if (con == null) {
	errorMessages = "You must establish a connection first!";
	throw new SQLException(errorMessages);
}

try {
	stmt4 = con.prepareStatement(selectSitewordQuery);
	stmt4.setString(1, site);
	stmt4.setString(2, word);
	// execute query
	rs = stmt4.executeQuery();
	int c = 0;
	while (rs.next()) {
		c++;
		}
	if (c == 1) {
		stmt4.close();
		rs.close();
		return true;
	} else {
		stmt4.close();
		rs.close();
		return false;
	}
	} catch (Exception e) {
 throw new Exception("Error: " + e.getMessage());
 }
}

/**
*H methodos validWord elegxei an i lexi tou site pou parsaroume uparxei ston pinaka siteword tis basis
*/

public boolean validWord (String word) throws Exception {

if (con == null) {
errorMessages = "You must establish a connection first!";
throw new SQLException(errorMessages);
}

try {
stmt4 = con.prepareStatement(selectWord2Query);
stmt4.setString(1, word);
// execute query
rs = stmt4.executeQuery();
int c = 0;
while (rs.next()) {
	c++;
	}
if (c == 1) {
	stmt4.close();
	rs.close();
	return true;
} else {
	stmt4.close();
	rs.close();
	return false;
}
} catch (Exception e) {
throw new Exception("Error: " + e.getMessage());
}
}

/**
*H methodos updateSiteword ananeonei ton pinaka siteword tis basis,
*me frequency = frequency + 1, afou i lexi uparxei idi sti vasi
*/

public void updateSiteword(String site, String word) throws SQLException {

	 if (con == null) {
		 errorMessages = "You must establish a connection first!";
		 throw new SQLException(errorMessages);
	 }

	 try {
		 stmt5 = con.prepareStatement(updateSitewordQuery);

		 stmt5.setString(1, site);
		 stmt5.setString(2, word);

		 // execute query
		 stmt5.executeUpdate();
		 stmt5.close();

	 } catch (Exception e4) {
		 errorMessages = "Error while inserting student to the database: <br>"
				 + e4.getMessage();
		 throw new SQLException(errorMessages);
	 }
 }

 /**
 *H methodos updateKeyword ananeonei ton pinaka siteword tis basis,
 *me keyword = 1 etsi oste na dixei oti i sigkekrimeni lexi einai kai keyword
 */

 public void updateKeyword(String site, String keyword) throws SQLException {

 	 if (con == null) {
 		 errorMessages = "You must establish a connection first!";
 		 throw new SQLException(errorMessages);
 	 }

 	 try {
 		 stmt5 = con.prepareStatement(updateKeywordQuery);

 		 stmt5.setString(1, site);
 		 stmt5.setString(2, keyword);

 		 // execute query
 		 stmt5.executeUpdate();
 		 stmt5.close();

 	 } catch (Exception e4) {
 		 errorMessages = "Error while inserting student to the database: <br>"
 				 + e4.getMessage();
 		 throw new SQLException(errorMessages);
 	 }
  }

	/**
  *H methodos updateSite ananeonei ton pinaka siteword tis basis,
  *bazontas ton neo titlo kai description tou site
  */

 public void updateSite(String site, String title, String description) throws SQLException {

 	 if (con == null) {
 		 errorMessages = "You must establish a connection first!";
 		 throw new SQLException(errorMessages);
 	 }

 	 try {
 		 stmt5 = con.prepareStatement(updateSiteQuery);

 		 stmt5.setString(1, title);
 		 stmt5.setString(2, description);
		 stmt5.setString(3, site);

 		 // execute query
 		 stmt5.executeUpdate();
 		 stmt5.close();

 	 } catch (Exception e4) {
 		 errorMessages = "Error while inserting student to the database: <br>"
 				 + e4.getMessage();
 		 throw new SQLException(errorMessages);
 	 }
  }

	/**
	*H methodos updateWord ananeonei ton pinaka word tis basis,
	*me frequency = frequency + 1, afou i lexi iparxei idi
	*/

 public void updateWord(String word) throws SQLException {

 	 if (con == null) {
 		 errorMessages = "You must establish a connection first!";
 		 throw new SQLException(errorMessages);
 	 }

 	 try {
 		 stmt5 = con.prepareStatement(updateWordQuery);

 		 stmt5.setString(1, word);

 		 // execute query
 		 stmt5.executeUpdate();
 		 stmt5.close();

 	 } catch (Exception e4) {
 		 errorMessages = "Error while inserting student to the database: <br>"
 				 + e4.getMessage();
 		 throw new SQLException(errorMessages);
 	 }
  }


}
