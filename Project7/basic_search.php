<html>

 <head><title>CS 564 PHP Project Search Result Page</title></head>

 <body>
   <tr>
     <td colspan="2" align="center" valign="top">
      Here are the search results (searched by title):<br>
       <table border="1" width="75%">
        <tr>
         <td align="center" bgcolor="#cccccc"><b>Univ ID</b></td>
         <td align="center" bgcolor="#cccccc"><b>Email Domain</b></td>
         <td align="center" bgcolor="#cccccc"><b>University Name</b></td>
         <td align="center" bgcolor="#cccccc"><b>Admit Season</b></td>
        </tr>	
 <?php
   // First check the itemid to see if it has been set
  if (! isset($_POST['admit_season'])) {
    echo "  <h3><i>Error, title not set to an acceptable value</i></h3>\n".
        " <a href=\"index.html\">Back to main page</a>\n".
	" </body>\n</html>\n";
    exit();
  }
  $admit_season = $_POST['admit_season'];
  // Connect to the Database
  pg_connect('dbname=cs564_f12 host=postgres.cs.wisc.edu') 
	or die ("Couldn't Connect ".pg_last_error()); 
  // Get category name and item counts
  $query = "SELECT * FROM itdepends_schema.university where admit_season='".$admit_season."'";
  // Execute the query and check for errors
  $result = pg_query($query);
  if (!$result) {
    $errormessage = pg_last_error();
    echo "Error with query: " . $errormessage;
    exit();
  }
  
  
  // get each row and print it out  
  while($row = pg_fetch_array($result,NULL,PGSQL_ASSOC))  {
    echo "        <tr>";
    echo "\n         <td align=\"center\">";
    echo "\n          ".$row['univ_id'];
    echo "\n         </td>";
    echo "\n         <td align=\"center\">";

    echo "\n          ".$row['email_domain'];
    echo "\n         </td>";
    echo "\n         <td align=\"center\">";
    echo "\n          ".$row['univ_name'];
    echo "\n         </td>";
    echo "\n         <td align=\"center\">";
    echo "\n          ".$row['admit_season'];
    echo "\n         </td>";
    echo "\n        </tr>";
  }
  pg_close();
?>
 </table>
     </td>
    </tr>
        <?php echo "<a href=\"index.html\">Back to main page</a>\n"?>
 </body>

</html>
