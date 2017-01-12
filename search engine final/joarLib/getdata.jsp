<%@ page language="java" contentType="text/html; charset=ISO-8859-7" pageEncoding="UTF-8"%>
<%@ page import="java.util.*,   javax.servlet.*, joarLib.*"%>

<%
  Joar_DB db = new Joar_DB();
  db.open();

	String query = request.getParameter("q");
	List<String> words = db.getData(query);

	Iterator<String> iterator = words.iterator();
	while(iterator.hasNext()) {
		String word = (String)iterator.next();
	  out.println(word);
	}

    db.close();
%>
