<%@ page language="java" contentType="text/html; charset=ISO-8859-7" pageEncoding="UTF-8"%>
<%@ page import="java.util.*,   javax.servlet.*, joarLib.*"%>
<%@ page errorPage="errorpage.jsp" %>
<%@ include file="header.jsp" %>

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
    </form>    <br>

<%
/** Το stem.jsp αρχείο εκτελεί την αναζήτηση Stem. Η Stem αναζήτηση χωρίζει κάθε
	* λέξη σε πρόθεμα(prefix), τη ρίζα της λέξης(root) και την κατάληξη(suffix) με στόχο
	* ο χρήστης να μπορεί να βρίσκει παράγωγη λέξη της λέξης που έχει αναζητήσει.
	* Χρησιμοποιούμε πίνακες με προθέματα και καταλήξεις που έχουν δημιουργηθεί με βάση τον αριθμό των γραμμάτων
	* του αντίστοιχου προθέματος, κατάληξης, με σκοπό να διευκολύνουμε την
	* ομαδοποίησή τους. Έχουμε παραλείψει αρκετά προθέματα και καταλήξεις γιατί δημιουργούν αστοχίες.
	* Επίσης, για την περίπτωση που υπάρχουν πολλές ομόρριζες λέξεις στην ίδια κλάση, υπάρχει κατάλληλη 
	* μέθοδος η οποία ομαδοποιεί τις λέξεις που προέρχονται από την ίδια ιστοσελίδα και αντί να 
	* τις επιστρέφει μια μια, τις επιστρέφει ομαδοποιημένες μαζί με το όνομα της ιστοσελίδας.
	* Στην περίπτωση που δωθούν από τον χρήστη περισσότερες από μια λέξεις, επιστρέφεται μήνυμα λάθους
	* καθώς η αναζήτηση Stem λειτουργεί μόνο με την εισαγωγή μιας λέξης.
	*
*/


  String word = request.getParameter("word");
  word = new String(word.getBytes("ISO-8859-1"), "ISO-8859-7");

  Joar_DB database = new Joar_DB();
  database.open();
  if (Boolean.FALSE.equals(database.validTrending(word))) {
    database.importTrending(word);
  } else {
    database.updateTrending(word);
  }

  boolean findSuffix = false;
  boolean findPrefix = false;
  String [] suffixes1 = {"s"};
  String [] suffixes2 = {"es", "ly", "er", "or", "al", "ty",  "ic", "en", "er", "ed"};
  String [] suffixes3 = {"ing", "ion", "ian", "ial", "ity", "oid", "ose", "ous", "ive",  "ful", "est", "ade", "age", "dom", "ify", "ism"};
  String [] suffixes4 = {"tion", "most", "word", "ware", "wise", "uous", "ure",  "ation", "ition", "ling", "itis", "ible", "able", "ness", "some", "ment", "ous", "ical", "eous", "ious", "less", "ship", "ance", "ence","ancy", "ency", "like", "hood"};
  String [] suffixes5 = {"ative", "itive", "acious", "arian", "esque", "ulent"};
  String [] suffixesmore = {"phobia", "ization"};

  String [] prefixes1 = {};
  String [] prefixes2 = {};
  String [] prefixes3 = {"dis"};
  String [] prefixes4 = {"over", "anti", "auto", "down", "semi", "tele", "mega"};
  String [] prefixes5= {"hyper", "extra", "under", "super", "trans", "ultra", "inter"};
  String [] prefixesmore = {};

  String[] words;
  words = word.split(" ");

  int n = words.length;
 %>

  <% if(n==1){
		  int len = word.length();

		  String preword = "";
		  String sufword = "";

      if (word.length()>=5 && findPrefix==false) {
        for (int i=0; i<prefixes5.length; i++) {
          if (word.substring(0, 5).equals(prefixes5[i])) {
            preword = word.substring(0, 5);
            word = word.substring(5, len);
            findPrefix = true;
            break;
          }
        }
      }

      if (word.length()>=4 && findPrefix==false) {
        for (int i=0; i<prefixes4.length; i++) {
          if (word.substring(0, 4).equals(prefixes4[i])) {
            preword = word.substring(0, 4);
            word = word.substring(4, len);
            findPrefix = true;
            break;
          }
        }
      }

      if (word.length()>=3 && findPrefix==false) {
        for (int i=0; i<prefixes3.length; i++) {
          if (word.substring(0, 3).equals(prefixes3[i])) {
            preword = word.substring(0, 3);
            word = word.substring(3, len);
            findPrefix = true;
            break;
          }
        }
      }

      if (word.length()>=2 && findPrefix==false) {
        for (int i=0; i<prefixes2.length; i++) {
          if (word.substring(0, 2).equals(prefixes2[i])) {
            preword = word.substring(0, 2);
            word = word.substring(2, len);
            findPrefix = true;
            break;
          }
        }
      }

        if (word.length()>=1 && findPrefix==false) {
          for (int i=0; i<prefixes1.length; i++) {
            if (word.substring(0, 1).equals(prefixes1[i])) {
              preword = word.substring(0, 1);
              word = word.substring(1, len);
              findPrefix = true;
              break;
            }
          }
        }

		  len = word.length();
		if (word.length()>=5) {
		  for (int i = 0; i < suffixes5.length; i++) {
  			if(word.substring(len - 5, len).equals(suffixes5[i])){
  			  sufword = word.substring(len - 5, len);
  			  word = word.substring(0,len - 5);
  			  findSuffix = true;
  			  break;
  			}
		  }
		}
	  if(findSuffix==false && word.length()>=4) {
			for (int i = 0; i < suffixes4.length; i++) {
  			  if(word.substring(len - 4, len).equals(suffixes4[i])){
  				sufword = word.substring(len - 4, len);
  				word = word.substring(0,len - 4);
  				findSuffix = true;
  				break;
  			  }
			}
		}

		  if(findSuffix==false && word.length()>=3) {
			     for (int i = 0; i < suffixes3.length; i++) {
			          if(word.substring(len - 3, len).equals(suffixes3[i])){
				              sufword = word.substring(len - 3, len);
				              word = word.substring(0,len - 3);
                      findSuffix = true;
				              break;
			          }
			      }
		  }

		  if(findSuffix==false &&  word.length()>=2) {
			for (int i = 0; i < suffixes2.length; i++) {
			  if(word.substring(len - 2, len).equals(suffixes2[i])){
				sufword = word.substring(len -2, len);
				word = word.substring(0,len - 2);
				findSuffix = true;
				break;
			  }
			}
		  }

		  if(findSuffix==false && word.length()>=1) {
			for (int i = 0; i < suffixes1.length; i++) {
			  if(word.substring(len - 1, len).equals(suffixes1[i])){
				sufword = word.substring(len - 1, len);
				word = word.substring(0,len - 1);
				findSuffix = true;
				break;
			  }
			}
		  }

		  // if (word.substring(len-2, len).equals("ed")) {
		  //   word = word.substring(0,len -2);
		  // }

		  database.open();

		  String stem = '%' + word + '%';
		  List<Siteword> sitewords = database.findWordForStem(stem);
		  Sites site2 = null;

		  %>
		  <h4>Αποτελέσματα αναζήτησης για: <b><%=preword%><span style="color:red;"><%=word%></span><%=sufword%></b></h4>
		  <%
			if (!sitewords.isEmpty()) {
			  for (Siteword siteword : sitewords) {
				site2 = database.findSite(siteword.getSite());%>
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
					
					
		      <% List<Word> relatives = database.getRelativesForStem(site2.getSite(),stem);  
                 int com = 1;
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
				Δεν βρέθηκαν αποτελέσματα - Επιστρέψτε σύντομα
			  </div>
		   <%}%>
	<%}else{%>
		  <div class='alert alert-danger' role='alert' style='text-align:center'>
		    Το stem λειτουργεί για την εισαγωγή μόνο μίας λέξης!
		  </div>

	 <%}%>

   <%
     database.close();
   %>

  </div> <!-- /container -->

<%@ include file="footer.jsp" %>
