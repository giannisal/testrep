<%@ page isErrorPage="true" %>
<%@ page language="java" contentType="text/html; charset=ISO-8859-7" pageEncoding="UTF-8"%>
<%@ page errorPage="errorpage.jsp" %>
      
    <div class='container theme-showcase' role='main'>
    </br>
	 </br>
      <div class='page-header'>
	    <h2  style="color:white">Σφάλμα</h2>
      </div>
      <div class="alert alert-danger" role="alert">
         <%= exception.getMessage() %>
      </div>
   </div>

  
 
  </body>
<html>	

