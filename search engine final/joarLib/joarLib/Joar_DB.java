package joarLib;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Joar_DB {

	private String errorMessages = "";
	private Connection con = null;
	private PreparedStatement stmt = null;
	private ResultSet rs = null;

	public String getErrorMessages() {
	  	return errorMessages;
  	}
	/**
     *Opens the connection with the database Server.Opens all Statements and
     * ResultSets. Finally, opens the connection with the Database Server.
     *
     * @throws SQLException
     *             (with the appropriate message) if any error occured.
	 */
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
	          "jdbc:mysql://195.251.249.131:3306/ismgroup77",
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
	 * � ������� getData �������� �� ������ �� ������ ��� ��������� ��� ������������ � �������
	 * ��� ���������� ����� �� ��� ������� ������ ��� �������� �� ���� �� ��������
	 *
	 * @param query
	 *            �� ������ ��� ��������� ��� ������������ � �������
	 *
	 * @return ����� �� ��� ������� ������ ��� �������� �� ���� �� ��������
	 *
	 * @throws Exception
	 *             (with the appropriate message) if any error occured.
	 */
	 public List<String> getData(String query) throws Exception {

		if (con == null) {
			 errorMessages = "You must establish a connection first!";
			 throw new SQLException(errorMessages);
		}

        try {
			 List<String> list = new ArrayList<String>();
             String selectWordQuery = "SELECT * FROM joar_word WHERE word LIKE ? ORDER BY frequency DESC;";
			 PreparedStatement stmt = con.prepareStatement(selectWordQuery);
			 query = query + "%";
			 stmt.setString(1, query);
			 // execute query
			 rs = stmt.executeQuery();
			 while (rs.next()) {
				//  if (rs.getString("word").startsWith(query)) {
					 String words = new String(rs.getString("word"));
					 list.add(words);
				// 	}
			 }

			 rs.close();
			 stmt.close();

			return list;
		} catch (Exception e) {
		    throw new Exception("Error: " + e.getMessage());
	    }
	}


	/**
	 * � ������� findWordForStem �������� �� ������ ��� ���� ��� ����� ��� ������������� � ������� ���
	 * ����������
	 *
	 * @param word
	 *            � ���� ��� ����� ��� ������������� � �������
	 *
	 * @return ����� �� �� ������ ��� link ��� ��� ��������� ��� ��������� ��� ����� ��� ����� ��� ������������� � �������
	 *
	 * @throws SQLException
	 *             (with the appropriate message) if any error occured.
	 */
	public List<Siteword> findWordForStem(String word) throws Exception {

		if (con == null) {
		errorMessages = "You must establish a connection first!";
		throw new SQLException(errorMessages);
		}

		 try {
			 List<Siteword> list = new ArrayList<Siteword>();
             String selectSitewordQuery = "SELECT count(site) as countSites,sum(frequency) as countFrequency , sum(keyword) as sumKeyword,site FROM joar_siteword WHERE word LIKE ? group by  site  ORDER BY  countSites DESC, sumKeyword DESC, countFrequency DESC";
			 PreparedStatement stmt = con.prepareStatement(selectSitewordQuery);

			 stmt.setString(1, word);
			 // execute query
			 rs = stmt.executeQuery();
			 while (rs.next()) {
				 Siteword words = new Siteword(rs.getString("site"));
				 list.add(words);
			 }

			 rs.close();
			 stmt.close();

			return list;
		 } catch (Exception e) {
		    throw new Exception("Error: " + e.getMessage());
	     }

    }

	/**
	 * � ������� getRelativesForStem ������� ��� site ��� ���� ��� ������������ ��� ��� ������ "findWordForStem"
	 * ��� ���������� �� ������ ��� ��������� ������ ��� �������� ��� ������������ site
	 *
	 * @param word
	 *            � ���� ��� ����� ��� ������������� � �������
	 * @param site
	 *            �� site ��� �������� ��� ��� stem.jsp �� ����� ����� ��� ��� ���� ��� ��������� � "findWordForStem"
	 *
	 * @return ����� �� �� ������ ��� link ��� ��� ��������� ��� ��������� ��� ����� ��� ����� ��� ������������� � �������
	 *
	 * @throws SQLException
	 *             (with the appropriate message) if any error occured.
	 */
	public List<Word> getRelativesForStem(String site, String word) throws Exception{
		if (con == null) {
			 errorMessages = "You must establish a connection first!";
			 throw new SQLException(errorMessages);
		}

		try{
			  List<Word> list = new ArrayList<Word>();
			  String finalQuery ="SELECT * FROM joar_siteword WHERE site = ? and word like ?;" ;
			  PreparedStatement stmt = con.prepareStatement(finalQuery);
			  stmt.setString(1,site);
			  stmt.setString(2,word);
			  ResultSet rs = stmt.executeQuery();
			  while(rs.next()){
				  Word relatives = new Word(rs.getString("word"));
				  list.add(relatives);
			  }
			  rs.close();
			  stmt.close();
			  return list;

		} catch(Exception ex) {
			 throw new Exception("An error occured while getting relatives from database: " + ex.getMessage());
		}
	}


	/**
	 * � ������� getSites ������� ��� ������ ��� ���� ���  ������ ��� �������� � ������� ��� ���������� �� ������� query
	 * ��� ������� ��� ������ ���������� �� ������� site ��� ����������� ��� ������ �����.
	 *
	 * @param query
	 *            �� query ��� �� ������ � prepareStatement ������������ �� ��������� ����� String
	 * @param words
	 *            ������� ��� ���������� �� ������ ��� ������ ��� ������������� � �������
	 *            ������������� �� ��������� ����� String
	 * @param n
	 *            �� ������ ��� ������ ��� �������� ������� �� ��� ������
	 *
	 * @return ����� �� �� ������ ��� link ��� ��������� ��� ������ ��� ������������� � �������
	 * @throws Exception
	 *             (with the appropriate message) if any error occured.
	 */
	public List<Siteword> getSites(String query, int n, String[] words) throws Exception {

			 if (con == null) {
				 errorMessages = "You must establish a connection first!";
				 throw new SQLException(errorMessages);
		     }
			 try{
                  List<Siteword> list = new ArrayList<Siteword>();
				  PreparedStatement stmt = con.prepareStatement(query);
				  for (int i=0; i<=n-1; i++){
					  stmt.setString(i+1,words[i]);
				  }
				  ResultSet rs = stmt.executeQuery();
				  while(rs.next()){
					  Siteword sites = new Siteword(rs.getString("site"));
					  list.add(sites);
				  }
				  rs.close();
				  stmt.close();
	              return list;

			  } catch(Exception ex) {
			     throw new Exception("An error occured while getting sites from database: " + ex.getMessage());
		      }
    }

	/**
	 * � ������� getRelatives ������� ��� site ��� ���� ��� ������������ ��� ��� ������ "getSites"
	 * ��� �� ��������� query ��� �������� ��� �� results.jsp ����� ��� ��� ������ �� ��� ������ ��� �������������
	 * � ������� ��� �� ������� ��� ������ �����, ��� ���������� �� ������ ��� ������ ��� �������� ��� site ����
	 * ��� ����� ��� ������������� � �������
	 *
	 * @param finalQuery
	 *            �� query ��� �� ������ � prepareStatement ������������ �� ��������� ����� String
	 * @param words
	 *            ������� ��� ���������� �� ������ ��� ������ ��� ������������� � �������
	 *            ������������� �� ��������� ����� String
	 * @param n
	 *            �� ������ ��� ������ ��� �������� ������� �� ��� ������
	 * @param link
	 *            �� site ��� �������� ��� ��� results.jsp �� ����� ����� ��� ��� ���� ��� ��������� � "getSites"
	 *
	 * @return ����� �� �� ������ ��� ������ ��� �������� �� site ��� ����� ��� ������������� � �������
	 * @throws Exception
	 *             (with the appropriate message) if any error occured.
	 *
	 */
	public List<Word> getRelatives (String link ,String finalQuery,String[] words, int n) throws Exception{
		if (con == null) {
			 errorMessages = "You must establish a connection first!";
			 throw new SQLException(errorMessages);
		}
		try{
			  List<Word> list = new ArrayList<Word>();
			  PreparedStatement stmt = con.prepareStatement(finalQuery);
			  stmt.setString(1,link);
			  for (int i=1; i<=n; i++){
				  stmt.setString(i+1,words[i-1]);
			  }
			  ResultSet rs = stmt.executeQuery();
			  while(rs.next()){
				  Word relatives = new Word(rs.getString("word"));
				  list.add(relatives);
			  }
			  rs.close();
			  stmt.close();
			  return list;

		} catch(Exception ex) {
			 throw new Exception("An error occured while getting relatives from database: " + ex.getMessage());
		}
	}

	/**
	 * � ������� findSite ������� ��� site ��� ���������� ��� ����� ��� ��� ��� ��������� ���
	 *
	 * @param site
	 *            �������� �� site �� ����� String
	 *
	 * @return ����� ����� Sites ��� �� ������������ site
	 * @throws Exception
	 *             (with the appropriate message) if any error occured.
	 *
	 */
	public Sites findSite(String site) throws Exception {

		if (con == null) {
			errorMessages = "You must establish a connection first!";
			throw new SQLException(errorMessages);
		}

		try {
			Sites site2 = null;
            String selectSite2Query = "SELECT * FROM joar_sites WHERE site = ?;";
			PreparedStatement stmt = con.prepareStatement(selectSite2Query);
			stmt.setString(1, site);
			// execute query
			rs = stmt.executeQuery();
			while (rs.next()) {
				site2 = new Sites(rs.getString("site"), rs.getString("title"), rs.getString("description"));
			}

			rs.close();
			stmt.close();

		    return site2;
		} catch (Exception e) {
	      throw new Exception("Error: " + e.getMessage());
        }

    }


    /**
 	 * � ������� validTrending ������� ��� String �� ��� ������ ������ ��� �������� � ������� ��� ���������� �� ������� query
 	 * ������� �� ���� �� ������ ������ ���� ���������� ������������ ��� ����� trending.
 	 *
 	 * @param word
 	 *            �� ������ ��� ������ ��� ������������� � �������
 	 *            ������������� �� ��������� ����� String
 	 *
 	 * @return boolean ��� �� �� ������ ������ � ���� �� ������������ ����������� ��� ����� treinding
	 *
 	 * @throws Exception
     *        (with the appropriate message) if any error occured.
 	 */
    public boolean validTrending (String word) throws Exception {

		if (con == null) {
		errorMessages = "You must establish a connection first!";
		throw new SQLException(errorMessages);
		}

		try {
			String selectTrendingQuery = "SELECT * FROM joar_trending WHERE word=?;";
			PreparedStatement stmt = con.prepareStatement(selectTrendingQuery);
			stmt.setString(1, word);
			// execute query
			rs = stmt.executeQuery();
			int c = 0;
			while (rs.next()) {
			c++;
			}
			if (c == 1) {
				stmt.close();
				rs.close();
				return true;
			} else {
				stmt.close();
				rs.close();
				return false;
			}
		} catch (Exception e) {
		   throw new Exception("Error: " + e.getMessage());
		}
    }


    /**
 	 * � ������� findTrending ��������� ��� �� ���� ���������
 	 * �� ����� �� �� Trending ������������ �� ���� ��� ���������� ����������,��������� ���������� ���
 	 * �� �������� ��������� ���������� .
 	 *
 	 * @param word
 	 *            �� ������ ��� ������ ��� ������������� � �������
 	 *            ������������� �� ��������� ����� String
 	 *
 	 * @return ����� �� �� Trending ������������
 	 * �� ���� ��� ���������� ����������,��������� ���������� ���
 	 * �� �������� ��������� ���������� .
	 *
 	 * @throws Exception
     *         (with the appropriate message) if any error occured.
 	 */
    public List<Trending> findTrending() throws Exception {

		if (con == null) {
			errorMessages = "You must establish a connection first!";
			throw new SQLException(errorMessages);
		}

		try {
			List<Trending> list = new ArrayList<Trending>();
			 String returnTrendingQuery = "SELECT * FROM joar_trending ORDER BY lastdate DESC, lastfrequency DESC , frequency DESC limit 10;";
			PreparedStatement stmt = con.prepareStatement(returnTrendingQuery);
			// execute query
			rs = stmt.executeQuery();
			while (rs.next()) {
				Trending trendings = new Trending(rs.getString("word"));
				list.add(trendings);
			}

			rs.close();
			stmt.close();

		   return list;
		} catch (Exception e) {
	      throw new Exception("Error: " + e.getMessage());
	    }
    }


    /**
 	 * � ������� importTrending ������� �� ������ ��� ������ ��� ���� ��������� ��� ���� ������ Trending.
 	 *
 	 * @param word
 	 *            �� ������ ��� ������ ��� ������������� � �������
 	 *            ������������� �� ��������� ����� String
 	 * @throws Exception
 	 *             (with the appropriate message) if any error occured.
 	 *
 	 */
 	public void importTrending(String word) throws Exception {

 		if (con == null) {
 			errorMessages = "You must establish a connection first!";
 			throw new SQLException(errorMessages);
 		}

 		try {
             String importTrendingQuery = "INSERT INTO joar_trending (word, frequency, lastdate, lastfrequency) VALUES (?, 1, CURDATE(), 1);";
 			PreparedStatement stmt = con.prepareStatement(importTrendingQuery);
 			stmt.setString(1, word);
 			// execute query
 			stmt.executeUpdate();
 			stmt.close();

 		} catch (Exception e) {
 	      throw new Exception("Error: " + e.getMessage());
        }
    }


    /**
 	 * � ������� updateTrending ���������� ��� ������ Trending ��� ������������ �� ��������� ���� ����� ���� ���� ���������� ��� ������� ���.
 	 *
 	 * @param word
 	 *            �� ������ ��� ������ ��� ������������� � �������
 	 *            ������������� �� ��������� ����� String
 	 * @throws SQLException
 	 *        (appropriate message) if any error occured.
 	 *
 	 */
    public void updateTrending(String word) throws SQLException {

		if (con == null) {
			errorMessages = "You must establish a connection first!";
			throw new SQLException(errorMessages);
		}

		try {
			String updateTrendingQuery = "UPDATE joar_trending SET frequency = frequency + 1, lastdate = CURDATE(), lastfrequency = lastfrequency + 1 WHERE word=?;";
			PreparedStatement stmt = con.prepareStatement(updateTrendingQuery);

			stmt.setString(1, word);

			// execute query
			stmt.executeUpdate();
			stmt.close();

		} catch (Exception e4) {
			errorMessages = "Error while updating word frequency to the database: <br>"
					+ e4.getMessage();
			throw new SQLException(errorMessages);
		}
    }

 }
