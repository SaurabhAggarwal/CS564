#include "catalog.h"
#include "query.h"


/**
 * FUNCTION: QU_Insert
 *
 * PURPOSE:  Inserts a record into the specified relation.
 *
 * PARAMETERS:
 *		relation 	(out)		Relation name in which record is to be inserted
 *		attrCnt		(in)		Number of attributes to be inserted
 *		attrList	(in)		Array of attribute informations to be inserted
 *	
 * RETURN VALUES:
 * 		Status	OK 				Records successfully inserted in the relation
 * 				UNIXERR 		The count of attributes in table and insert query do not match
 *				BADCATPARM 		Relation name is empty
 *				BADSCANPARM  	Error in allocating page: All buffer frames are pinned
 *				FILEEOF 		Reached the end of file while scanning for the record
 *				BUFFEREXCEEDED  All buffer frames are pinned
 *				HASHTBLERROR	Hash table error occurred
 *				PAGENOTPINNED 	Pin count is already 0
 *				HASHNOTFOUND  	Page is not in the buffer pool hash table
 **/
const Status QU_Insert(	const string & relation, 
						const int attrCnt, 
						const attrInfo attrList[])
{
  	AttrDesc *attrs;
  	int actualAttrCnt;
  	Status status;
	
  	if((status = attrCat->getRelInfo(relation, actualAttrCnt, attrs)) != OK)
    	return status;

	if(actualAttrCnt != attrCnt)
		return UNIXERR;
		
  	int reclen = 0;
  	for(int i = 0; i < attrCnt; i++)
    	reclen += attrs[i].attrLen;

  	InsertFileScan ifs(relation, status);
  	ASSERT(status == OK);

  	char *insertData;
  	if(!(insertData = new char [reclen]))
  		return INSUFMEM;
  	
	int insertOffset = 0;
	int value = 0;
	float fValue = 0;
	for(int i = 0; i < attrCnt; i++)
	{
		bool attrFound = false;
		for(int j = 0; j < attrCnt; j++)
		{
			if(strcmp(attrs[i].attrName, attrList[j].attrName) == 0)
			{
				insertOffset = attrs[i].attrOffset;
				
				switch(attrList[j].attrType)
				{
					case STRING: 
						memcpy((char *)insertData + insertOffset, (char *)attrList[j].attrValue, attrs[i].attrLen);
						break;
				 		
					case INTEGER: 
						value = atoi((char *)attrList[j].attrValue);
				 		memcpy((char *)insertData + insertOffset, &value, attrs[i].attrLen);
				 		break;
				 		
					case FLOAT: 
						fValue = atof((char *)attrList[j].attrValue);		
						memcpy((char *)insertData + insertOffset, &fValue, attrs[i].attrLen);
				 		break;
				}
				
				attrFound = true;
				break;
			}
		}
		
		if(attrFound == false)
		{
			delete [] insertData;
			free(attrs);
			return UNIXERR;
		}
	}
	
  	Record insertRec;
  	insertRec.data = (void *) insertData;
  	insertRec.length = reclen;
	
  	RID insertRID;
  	status = ifs.insertRecord(insertRec, insertRID);
	
	delete [] insertData;
	free(attrs);
	
	return status;
}
