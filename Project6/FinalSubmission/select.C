#include "catalog.h"
#include "query.h"
#include <stdlib.h>
#include "stdio.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);


/**
 * FUNCTION: QU_Select
 *
 * PURPOSE:  Selects records from the specified relation.
 *
 * PARAMETERS:
 *		result 		(out)		Relation name where result is to be stored
 *		projCnt		(in)		Count of projections
 *		projNames	(in)		Information array of projections
 *		attr		(in)		Information about the attribute to be matched
 *		op			(in)		Operator to be used for matching
 *		attrValue	(in)		Value to be used for matching
 *	
 * RETURN VALUES:
 *		Status  	OK         		Selected records successfully found and returned
 *                  BADCATPARM      Relation name is empty
 *                  BADSCANPARM     Error in allocating page: All buffer frames are pinned
 *                  FILEEOF         Reached the end of file while scanning for the record
 *                  BUFFEREXCEEDED  All buffer frames are pinned
 *                  HASHTBLERROR    Hash table error occurred
 *                  PAGENOTPINNED   Pin count is already 0
 *                  HASHNOTFOUND    Page is not in the buffer pool hash table
 **/

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 			//If null, unconditional scan
		       const Operator op, 
		       const char *attrValue)
{
   	// Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	
	Status status;
	AttrDesc projNames_Descs[projCnt];
	
	for (int i = 0; i < projCnt; i++)
	{
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projNames_Descs[i]);
	    if (status != OK)
			return status;
	}
	
	AttrDesc *attrDescWhere = NULL;
	int attrValueLen = 0;
	if(attr != NULL)
	{
		attrDescWhere = new AttrDesc;
		status = attrCat->getInfo(attr->relName, attr->attrName, *attrDescWhere);
		attrValueLen = attrDescWhere->attrLen;
		if (status != OK)
			return status;
	}
	
	return ScanSelect(	result, 
						projCnt, 
						projNames_Descs,
						attrDescWhere, 
						op,
						attrValue,
	  					attrValueLen);
}


/**
 * FUNCTION: ScanSelect
 *
 * PURPOSE:  Selects records from the specified relation.
 *
 * PARAMETERS:
 *		result 			(out)		Relation name where result is to be stored
 *		projCnt			(in)		Count of projections
 *		projNames_Descs	(in)		Attribute description array of projections
 *		filterAttr		(in)		Attribute description of the attribute to be matched
 *		op				(in)		Operator to be used for matching
 *		filterValue		(in)		Value to be used for matching
 *		reclen			(in)		Length of the filter value
 *	
 * RETURN VALUES:
 *		Status  	OK         		Selected records successfully found and returned
 *                  BADCATPARM      Relation name is empty
 *                  BADSCANPARM     Error in allocating page: All buffer frames are pinned
 *                  FILEEOF         Reached the end of file while scanning for the record
 *                  BUFFEREXCEEDED  All buffer frames are pinned
 *                  HASHTBLERROR    Hash table error occurred
 *                  PAGENOTPINNED   Pin count is already 0
 *                  HASHNOTFOUND    Page is not in the buffer pool hash table
 **/
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames_Descs[],
			const AttrDesc *filterAttr, 
			const Operator op, 
			const char *filterValue,
			const int reclen)
{
	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Record rec;
	RID rid;
	Status status;
	
	HeapFileScan* hfs = new HeapFileScan(projNames_Descs[0].relName, status);
	if (status != OK)
	{
		delete hfs;
		return status;
	}

	if(filterAttr == NULL)
	{
		if ((status = hfs->startScan(0, 0, STRING,  NULL, EQ)) != OK)
		{
			delete hfs;
			return status;
		}
	}
	else
	{
		int intValue;
		float floatValue;
		switch(filterAttr->attrType)
		{
			case STRING:
				status = hfs->startScan(filterAttr->attrOffset, filterAttr->attrLen, (Datatype) filterAttr->attrType, filterValue, (Operator) op);
				break;
		
			case INTEGER:
		 		intValue = atoi(filterValue);
				status = hfs->startScan(filterAttr->attrOffset, filterAttr->attrLen, (Datatype) filterAttr->attrType, (char *)&intValue, (Operator) op);
				break;
		
			case FLOAT:
				floatValue = atof(filterValue);
				status = hfs->startScan(filterAttr->attrOffset, filterAttr->attrLen, (Datatype) filterAttr->attrType, (char *)&floatValue, (Operator) op);
				break;
		}
		
		if(status != OK)
		{
			delete hfs;
			return status;
		}
	}
	
	while ((status = hfs->scanNext(rid)) == OK)
	{
 		if (status == OK)
  		{
  			status = hfs->getRecord(rec);
			if (status != OK)
				break;
          	
    		attrInfo attrList[projCnt];
			int value = 0; char buffer[33];
			float fValue;
    		for(int i = 0; i < projCnt; i++)
			{
				AttrDesc attrDesc = projNames_Descs[i];
  	  			
  	  			strcpy(attrList[i].relName, attrDesc.relName);
  	  			strcpy(attrList[i].attrName, attrDesc.attrName);
  	  			attrList[i].attrType = attrDesc.attrType;
  	  			attrList[i].attrLen = attrDesc.attrLen;
  	  			
  	  			attrList[i].attrValue = (void *) malloc(attrDesc.attrLen);
  	  			
				switch(attrList[i].attrType)
				{
					case STRING: 
						 memcpy((char *)attrList[i].attrValue, (char *)(rec.data + attrDesc.attrOffset), attrDesc.attrLen);
						 break;
						 
					case INTEGER: 
						memcpy(&value, (int *)(rec.data + attrDesc.attrOffset), attrDesc.attrLen);
						sprintf((char *)attrList[i].attrValue, "%d", value);
						break;
						 
					case FLOAT: 
 						memcpy(&fValue, (float *)(rec.data + attrDesc.attrOffset), attrDesc.attrLen);
 						sprintf((char *)attrList[i].attrValue, "%f", fValue);
						break;
				}
  			}
  		
  			status = QU_Insert(result, projCnt, attrList);
  			if(status != OK)
			{
				delete hfs;
				return status;
			}
  		}
	}
		
	return OK;
}
