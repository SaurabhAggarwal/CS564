#include <sys/types.h>
#include <functional>
#include <string.h>
#include <stdio.h>
using namespace std;

#include "error.h"
#include "utility.h"
#include "catalog.h"

// define if debug output wanted


//
// Retrieves and prints information from the catalogs about the for the
// user. If no relation is given (relation is NULL), then it lists all
// the relations in the database, along with the width in bytes of the
// relation, the number of attributes in the relation, and the number of
// attributes that are indexed.  If a relation is given, then it lists
// all of the attributes of the relation, as well as its type, length,
// and offset, whether it's indexed or not, and its index number.
//
// Returns:
// 	OK on success
// 	error code otherwise
//

#ifdef DEBUG
const Status RelCatalog::help(const string & relation)
{
  Status status;
  RelDesc rd;
  AttrDesc *attrs;

  int attrCnt, i;

  if (relation.empty()) return UT_Print(RELCATNAME);

	if ((status = relCat->getInfo(relation, rd)) != OK) return status;
	
	if ((status = attrCat->getRelInfo(relation, attrCnt, attrs)) != OK) return status;
	
	printf("\n");
	printf("%-20s%-12s%-12s%-12s\n", "attrName", "attrType", "attrOffset", "attrLen");
	printf("%-20s%-12s%-12s%-12s\n", "-------------------", "-----------", "-----------", "-----------");
	for (i = 0; i < attrCnt; i++)
	{ 
		char type[20];
		switch(attrs[i].attrType)
		{
			case 0: strcpy(type, "string"); break;
			case 1: strcpy(type, "int"); break;
			case 2: strcpy(type, "float"); break;
		}
		printf("%-20s%-12s%-12d%-12d\n", attrs[i].attrName, type, attrs[i].attrOffset, attrs[i].attrLen);
	}
	
	delete []attrs;
	
	return OK;
}
#endif

