#include "catalog.h"


const Status RelCatalog::createRel(const string & relation, 
				   const int attrCnt,
				   const attrInfo attrList[])
{
  Status status;
  RelDesc rd;
  AttrDesc ad;

  if (relation.empty() || attrCnt < 1)
    return BADCATPARM;

  if (relation.length() >= sizeof rd.relName)
    return NAMETOOLONG;

	status = relCat->getInfo(relation, rd);
	if(status == OK)
	{
		return RELEXISTS;
	}
	else
	{
		// Adding a tuple in "relcat" table
		strcpy(rd.relName, relation.c_str());
		rd.attrCnt = attrCnt;
		status = addInfo(rd);
		
		// Adding appropriate tuples in the "attrcat" table
		int offset = 0;
		for(int i = 0; i< attrCnt; i++)
		{
			strcpy(ad.relName, attrList[i].relName);
			strcpy(ad.attrName, attrList[i].attrName);
			ad.attrOffset = offset;//(i == 0) ? 0 : MAXNAME; 
			ad.attrType = attrList[i].attrType; 
			ad.attrLen = attrList[i].attrLen;
			offset += ad.attrLen;
			
			status = attrCat->addInfo(ad);
			if(status != OK) break;
		}
	}
	// Creating HeapFile for the relation
	status = createHeapFile(relation.c_str());

	return status;
}
