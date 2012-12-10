<html>

 <head><title>CS 564 PHP Project Search Result Page</title></head>

 <body>
 <?php
   // First check the itemid to see if it has been set
  if (! isset($_POST['username'])) {
    echo "  <h3><i>Error, username not set to an acceptable value</i></h3>\n".
        " <a href=\"index.html\">Back to main page</a>\n".
	" </body>\n</html>\n";
    exit();
  }
  $username = $_POST['username'];
  // Connect to the Database
  pg_connect('dbname=cs564_f12 host=postgres.cs.wisc.edu') 
	or die ("Couldn't Connect ".pg_last_error()); 
  // Get category name and item counts
  $query = "delete from itdepends_schema.student where username='".$username."'";
  // Execute the query and check for errors
  $result = pg_query($query);
  if (!$result) {
    $errormessage = pg_last_error();
    echo "Error with query: " . $errormessage;
    exit();
  }
  echo "Successfully deleted entry for user: ".$username."<br><br>";
  
  
  // get each row and print it out  
  
  pg_close();
?>
 </table>
     </td>
    </tr>
        <?php echo "<a href=\"index.html\">Back to main page</a>\n"?>
 </body>

</html>
