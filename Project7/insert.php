<html>

 <head><title>CS 564 PHP Project Search Result Page</title></head>

 <body>
 <?php
   // First check the itemid to see if it has been set
  if (! isset($_POST['undergrad_gpa']) || ! isset($_POST['gre_score']) || ! isset($_POST['toefl_score']) || ! isset($_POST['work_exp']) || ! isset($_POST['app_term'])) {
    echo "  <h3><i>Error, required parameters not set to an acceptable value</i></h3>\n".
        " <a href=\"index.html\">Back to main page</a>\n".
	" </body>\n</html>\n";
    exit();
  }
  
  $username = $_POST['username'];
  $email_id = $_POST['email_id'];
  $name = $_POST['name'];
  $undergrad_gpa = $_POST['undergrad_gpa'];
  $gre_score = $_POST['gre_score'];
  $toefl_score = $_POST['toefl_score'];
  $work_exp = $_POST['work_exp'];
  $app_term = $_POST['app_term'];
  
  // Connect to the Database
  pg_connect('dbname=cs564_f12 host=postgres.cs.wisc.edu') 
	or die ("Couldn't Connect ".pg_last_error()); 
  // Get category name and item counts
  # $query = "SELECT * FROM itdepends_schema.university where admit_season='".$admit_season."'";
  $query = "insert into itdepends_schema.student values('".$username."', '".$email_id."', '".$name."', ".$undergrad_gpa.", ".$toefl_score.", ".$gre_score.", ".$work_exp.", '".$app_term."')";
  
  // Execute the query and check for errors
  $result = pg_query($query);
  if (!$result) {
    $errormessage = pg_last_error();
    echo "Error with query: " . $errormessage;
    exit();
  }
  echo "Successfully inserted entry for user: ".$username."<br><br>";
  
  pg_close();
?>
 </table>
     </td>
    </tr>
        <?php echo "<a href=\"index.html\">Back to main page</a>\n"?>
 </body>

</html>
