#include "catalog.h"
#include "query.h"


/**
 * FUNCTION: QU_Delete
 *
 * PURPOSE:  Deletes records from a specified relation.
 *
 * PARAMETERS:
 *		relation		(out)		Relation name from where records are to be deleted
 *		attrName		(in)		Name of the attribute to match when deleting records
 *		op				(in)		Operator to be used for matching
 *		type			(in)		Datatype of the attribute
 *		attrValue		(in)		Value to be used for matching
 *	
 * RETURN VALUES:
 * 		Status	OK 					Records successfully deleted from the relation
 *				BADCATPARM 			Relation name is empty
 *				BADSCANPARM  		Error in allocating page: All buffer frames are pinned
 *				FILEEOF 			Reached the end of file while scanning for the record
 *				BUFFEREXCEEDED  	All buffer frames are pinned
 *				HASHTBLERROR		Hash table error occurred
 *				PAGENOTPINNED 		Pin count is already 0
 *				HASHNOTFOUND  		Page is not in the buffer pool hash table
 **/
const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	Status status;
  	HeapFileScan* hfs = new HeapFileScan(relation, status);
  	if(status != OK)
  		return status;
  
  	AttrDesc attrDesc;
  	RID rid;
  	
  	attrCat->getInfo(relation, attrName, attrDesc);
	
	int offset = attrDesc.attrOffset;
	int length = attrDesc.attrLen;
	
	int intValue;
	float floatValue;
	
	switch(type)
	{
		case STRING:
			status = hfs->startScan(offset, length, type, attrValue, op);
			break;
		
		case INTEGER:
		 	intValue = atoi(attrValue);
			status = hfs->startScan(offset, length, type, (char *)&intValue, op);
			break;
		
		case FLOAT:
			floatValue = atof(attrValue);
			status = hfs->startScan(offset, length, type, (char *)&floatValue, op);
			break;
	}
		
  	if (status != OK)
  	{
    	delete hfs;
    	return status;
  	}
  	
  	while((status = hfs->scanNext(rid)) == OK) 
  	{
    	if ((status = hfs->deleteRecord()) != OK)
    		return status;
  	}

	hfs->endScan();
    delete hfs;
  	
  	return OK;
}


