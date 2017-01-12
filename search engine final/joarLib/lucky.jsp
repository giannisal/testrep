<%@ page language="java" contentType="text/html; charset=ISO-8859-7" pageEncoding="UTF-8"%>
<%@ page import="java.util.*,   javax.servlet.*, joarLib.*"%>
<%@ page errorPage="errorpage.jsp" %>
<%@ include file="header.jsp" %>

<%
	String word = request.getParameter("word");
	word = new String(word.getBytes("ISO-8859-1"), "ISO-8859-7");
	
	String[] words;
	words = word.split(" ");

    int n = words.length;
	
	String cond;
	if(n==1) cond = "word=?";
    else  cond = "word=? OR ";
	
    for(int i = 2;i<=n;i++)	{
		if(i==n) 
			cond += "word=?";
		else
			cond += "word=? OR ";
	}
	
	String query ="Select count(site) as countSites,sum(frequency) as countSumFrequency,site "+
			                  "from joar_siteword " +  
							  "where " + cond + " group by site order by countSites DESC, countSumFrequency DESC";
	
	Joar_DB database = new Joar_DB();
	database.open();
    List<Siteword> sitewords = database.getSites(query,n,words);
  database.close();

  if (sitewords == null || sitewords.isEmpty()) { %>
    <jsp:forward page="noLucky.jsp"/>
  <%} else {
    response.sendRedirect(sitewords.get(0).getSite());
  }%>
