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

	String query ="Select count(site) as countSites,sum(frequency) as countSumFrequency,sum(keyword) as sumKeyword , site "+
			                  "from joar_siteword " +
							  "where " + cond + " group by site order by countSites DESC, sumKeyword DESC,  countSumFrequency DESC";

	Joar_DB database = new Joar_DB();
	database.open();
    List<Siteword> sitewords = database.getSites(query,n,words);

  if (Boolean.FALSE.equals(database.validTrending(word))) {
    database.importTrending(word);
  } else {
    database.updateTrending(word);
  }

    Sites site2 = null;
%>
  <div class="container theme-showcase" role="main">
     <form  class='form-inline' method = 'get' action = 'results.jsp'>
      <div class="form-group">
        <a href="index.jsp">
          <img class="img-responsive img-rounded" src="../joar/logojoar.PNG" height="40" width="80" alt="j0ar Logo">
        </a>
      </div>
      <div class="form-group">
        <input type="text" class="form-control" id="word" name="word"/>

        <script>
          $("#word").autocomplete("getdata.jsp", {maxItemsToShow : 5});
        </script>
      </div>
      <button type="submit" class="btn btn-primary">Αναζήτηση</button>
    </form>
    <br>
    <h4>Αποτελέσματα αναζήτησης για: <b><%=word%></b></h4>
    <%
    if (!sitewords.isEmpty()) {
      int com;
      for (Siteword siteword : sitewords) {
        site2 = database.findSite(siteword.getSite());
        %>
          <h3><a href="<%=site2.getSite()%>"><%=site2.getTitle()%></a></h3>
          <p>
            <% if (site2.getSite() != null) {%>
              <span style="color:green;"><%=site2.getSite()%></span>
            <%}%>

            <% if (site2.getDescription() != null) {%>
              - <%=site2.getDescription()%>
            <%}%>
          </p>
		  <p style="color:grey;">Relatives:
		      <% String finalQuery ="Select * from joar_siteword where site = ? AND ( " + cond + ")" ;
			     List<Word> relatives = database.getRelatives (site2.getSite() ,finalQuery, words,n);
                 com = 1;
			     for(Word relative : relatives){
					  if (com != relatives.size()) {%>
						<b><%=relative.getName()%></b>,
					<%}
					  else {%>
						<b><%=relative.getName()%></b>
					<%}
                     com++;
			  }%>
		  </p>
      <%}
    } else {%>
      <div class='alert alert-warning' role='alert' style='text-align:center'>
        Δεν βρέθηκαν αποτελέσματα - Κάντε αναζήτηση <a href="http://ism.dmst.aueb.gr/ismgroup77/joar/stem.jsp?word=<%=word%>">Stem</a>
      </div>
    <%}
      database.close();%>
  </div> <!-- /container -->

<%@ include file="footer.jsp" %>
