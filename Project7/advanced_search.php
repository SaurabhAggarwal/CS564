<html>

 <head><title>CS 564 PHP Project Search Result Page</title></head>

 <body>
   <tr>
     <td colspan="2" align="center" valign="top">
      Here are the search results:<br>
       <table border="1" width="75%">
        <tr>
         <td align="center" bgcolor="#cccccc"><b>Undergrad GPA</b></td>
         <td align="center" bgcolor="#cccccc"><b>GRE Score</b></td>
         <td align="center" bgcolor="#cccccc"><b>TOEFL Score</b></td>
         <td align="center" bgcolor="#cccccc"><b>Work Experience</b></td>
         <td align="center" bgcolor="#cccccc"><b>University</b></td>
        </tr>	
 <?php
   // First check the itemid to see if it has been set
  if (! isset($_POST['undergrad_gpa']) && ! isset($_POST['gre_score']) && ! isset($_POST['toefl_score']) && ! isset($_POST['work_exp'])) {
    echo "  <h3><i>Error, required parameters not set to an acceptable value</i></h3>\n".
        " <a href=\"index.html\">Back to main page</a>\n".
	" </body>\n</html>\n";
    exit();
  }
  
  $undergrad_gpa = $_POST['undergrad_gpa'];
  $gre_score = $_POST['gre_score'];
  $toefl_score = $_POST['toefl_score'];
  $work_exp = $_POST['work_exp'];
  
  // Connect to the Database
  pg_connect('dbname=cs564_f12 host=postgres.cs.wisc.edu') 
	or die ("Couldn't Connect ".pg_last_error()); 
  // Get category name and item counts
  # $query = "SELECT * FROM itdepends_schema.university where admit_season='".$admit_season."'";
  $query = "select S.undergrad_gpa, S.gre_score, S.toefl_score, S.work_experience, U.univ_name from itdepends_schema.student S, itdepends_schema.studying_in T, itdepends_schema.University U where S.username=T.username AND T.univ_id=U.univ_id AND S.undergrad_gpa > ".$undergrad_gpa." AND S.gre_score > ".$gre_score."-15 AND S.gre_score < ".$gre_score."+15 AND S.toefl_score > ".$toefl_score."-10 AND S.toefl_score < ".$toefl_score."+10 AND work_experience > ".$work_exp."-2 AND work_experience < ".$work_exp."+2 order by gre_score DESC";

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
    echo "\n          ".$row['undergrad_gpa'];
    echo "\n         </td>";
    echo "\n         <td align=\"center\">";
    echo "\n          ".$row['gre_score'];
    echo "\n         </td>";
    echo "\n         <td align=\"center\">";
    echo "\n          ".$row['toefl_score'];
    echo "\n         </td>";
    echo "\n         <td align=\"center\">";
    echo "\n          ".$row['work_experience'];
    echo "\n         </td>";
    echo "\n         <td align=\"center\">";
    echo "\n          ".$row['univ_name'];
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
